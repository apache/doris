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
#include "runtime/runtime_profile.h"
#include "service/http/http_client.h"
#include "util/defer_op.h"
#include "util/security.h"

namespace doris {

struct AIExecutionStatistics::Impl {
    RuntimeProfile profile {"AI_FUNCTION"};
    RuntimeProfile::Counter* input_rows =
            profile.add_counter_with_level("AIFunctionInputRows", TUnit::UNIT, 1);
    RuntimeProfile::Counter* null_rows =
            profile.add_counter_with_level("AIFunctionSkippedNullRows", TUnit::UNIT, 1);
    RuntimeProfile::Counter* logical =
            profile.add_counter_with_level("AIFunctionLogicalRequestCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* first =
            profile.add_counter_with_level("AIFunctionFirstAttemptCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* attempts =
            profile.add_counter_with_level("AIFunctionRequestCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* retries =
            profile.add_counter_with_level("AIFunctionRetryCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* errors =
            profile.add_counter_with_level("AIFunctionErrorCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* timeouts =
            profile.add_counter_with_level("AIFunctionTimeoutCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* cancelled =
            profile.add_counter_with_level("AIFunctionCancelledCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* parse_errors =
            profile.add_counter_with_level("AIFunctionParseErrorCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* parse_time =
            profile.add_counter_with_level("AIFunctionParseTime", TUnit::TIME_NS, 1);
    RuntimeProfile::Counter* retry_wait_ns =
            profile.add_counter_with_level("AIFunctionRetryWaitTime", TUnit::TIME_NS, 1);
    RuntimeProfile::Counter* http_time_ns =
            profile.add_counter_with_level("AIFunctionHttpTime", TUnit::TIME_NS, 1);
    RuntimeProfile::Counter* dns_time_ns =
            profile.add_counter_with_level("AIFunctionDnsTime", TUnit::TIME_NS, 1);
    RuntimeProfile::Counter* connect_time_ns =
            profile.add_counter_with_level("AIFunctionTcpConnectTime", TUnit::TIME_NS, 1);
    RuntimeProfile::Counter* tls_time_ns =
            profile.add_counter_with_level("AIFunctionTlsHandshakeTime", TUnit::TIME_NS, 1);
    RuntimeProfile::Counter* first_byte_time_ns =
            profile.add_counter_with_level("AIFunctionFirstByteTime", TUnit::TIME_NS, 1);
    RuntimeProfile::Counter* usage_parse_time_ns =
            profile.add_counter_with_level("AIFunctionUsageParseTime", TUnit::TIME_NS, 1);
    RuntimeProfile::Counter* new_connections =
            profile.add_counter_with_level("AIFunctionNewConnectionCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* prompt_tokens =
            profile.add_counter_with_level("AIFunctionPromptTokens", TUnit::UNIT, 1);
    RuntimeProfile::Counter* completion_tokens =
            profile.add_counter_with_level("AIFunctionCompletionTokens", TUnit::UNIT, 1);
    RuntimeProfile::Counter* total_tokens =
            profile.add_counter_with_level("AIFunctionTotalTokens", TUnit::UNIT, 1);
    RuntimeProfile::Counter* prompt_usage_count =
            profile.add_counter_with_level("AIFunctionPromptUsageCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* completion_usage_count =
            profile.add_counter_with_level("AIFunctionCompletionUsageCount", TUnit::UNIT, 1);
    RuntimeProfile::Counter* total_usage_count =
            profile.add_counter_with_level("AIFunctionTotalUsageCount", TUnit::UNIT, 1);
};

AIExecutionStatistics::AIExecutionStatistics() : _impl(std::make_unique<Impl>()) {}
AIExecutionStatistics::~AIExecutionStatistics() = default;
RuntimeProfile* AIExecutionStatistics::profile() {
    return &_impl->profile;
}

void AIExecutionStatistics::update_profile_snapshot(TRuntimeProfileTree& fragment_profile,
                                                    int profile_level) {
    TRuntimeProfileTree ai_profile;
    _impl->profile.to_thrift(&ai_profile, profile_level);
    auto& root = fragment_profile.nodes.front();
    const auto& ai_root = ai_profile.nodes.front();
    std::erase_if(root.counters,
                  [](const auto& counter) { return counter.name.starts_with("AIFunction"); });
    for (auto& [parent, children] : root.child_counters_map) {
        std::erase_if(children, [](const auto& child) { return child.starts_with("AIFunction"); });
    }
    for (const auto& counter : ai_root.counters) {
        if (counter.name.starts_with("AIFunction")) {
            root.counters.push_back(counter);
        }
    }
    for (const auto& [parent, children] : ai_root.child_counters_map) {
        for (const auto& child : children) {
            if (child.starts_with("AIFunction")) {
                root.child_counters_map[parent].insert(child);
            }
        }
    }
}

void AIExecutionStatistics::record_input(int64_t rows, int64_t skipped_nulls) {
    _impl->input_rows->update(rows);
    _impl->null_rows->update(skipped_nulls);
}

void AIExecutionStatistics::record_parse(int64_t elapsed_ns, bool failed) {
    _impl->parse_time->update(elapsed_ns);
    _impl->parse_errors->update(failed);
    _impl->errors->update(failed);
}

void AIExecutionStatistics::record_request(const AIRequestStats& stats, const Status& status,
                                           bool cancelled) {
    _impl->logical->update(1);
    _impl->first->update(stats.attempts > 0);
    _impl->attempts->update(stats.attempts);
    _impl->retries->update(stats.attempts - (stats.attempts > 0));
    _impl->errors->update(!status.ok());
    _impl->timeouts->update(status.is<ErrorCode::TIMEOUT>());
    _impl->cancelled->update(cancelled && !status.ok() && !status.is<ErrorCode::TIMEOUT>());
    _impl->retry_wait_ns->update(stats.retry_wait_ns);
    _impl->http_time_ns->update(stats.http_time_ns);
    _impl->dns_time_ns->update(stats.dns_time_ns);
    _impl->connect_time_ns->update(stats.connect_time_ns);
    _impl->tls_time_ns->update(stats.tls_time_ns);
    _impl->first_byte_time_ns->update(stats.first_byte_time_ns);
    _impl->usage_parse_time_ns->update(stats.usage_parse_time_ns);
    _impl->new_connections->update(stats.new_connections);
    _impl->prompt_tokens->update(stats.prompt_tokens);
    _impl->completion_tokens->update(stats.completion_tokens);
    _impl->total_tokens->update(stats.total_tokens);
    _impl->prompt_usage_count->update(stats.prompt_usage_count);
    _impl->completion_usage_count->update(stats.completion_usage_count);
    _impl->total_usage_count->update(stats.total_usage_count);
}

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

AIRequestExecutor::AIRequestExecutor(std::shared_ptr<AIExecutionStatistics> statistics)
        : _now([] { return Clock::now(); }),
          _sleep([](Clock::duration delay) { std::this_thread::sleep_for(delay); }),
          _clients(std::make_unique<ClientCache>()),
          _statistics(std::move(statistics)) {}
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
                                         Clock::time_point deadline, AIRequestStats& stats,
                                         bool collect_statistics) const {
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
        // Retry waiting is required even when its elapsed time is not being collected.
        if (collect_statistics) {
            const auto elapsed = _now() - started;
            stats.retry_wait_ns +=
                    std::chrono::duration_cast<std::chrono::nanoseconds>(elapsed).count();
        }
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
        Clock::time_point deadline, AIRequestStats& stats, bool collect_statistics) {
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
    // Large EMBED responses must not build an extra JSON DOM when nobody requests usage.
    if (!collect_statistics) {
        return status;
    }
    const auto metrics = client->get_transfer_metrics();
    stats.http_time_ns += metrics.total_time_ns;
    stats.dns_time_ns += metrics.dns_time_ns;
    stats.connect_time_ns += metrics.connect_time_ns;
    stats.tls_time_ns += metrics.tls_time_ns;
    stats.first_byte_time_ns += metrics.first_byte_time_ns;
    stats.new_connections += metrics.new_connections;
    // Inspect every attempt, including provider errors, before response reuse or cancellation.
    const auto parse_started = Clock::now();
    const auto usage = adapter.parse_usage(response);
    stats.usage_parse_time_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - parse_started)
                    .count();
    if (usage.prompt_tokens) {
        stats.prompt_tokens += *usage.prompt_tokens;
        ++stats.prompt_usage_count;
    }
    if (usage.completion_tokens) {
        stats.completion_tokens += *usage.completion_tokens;
        ++stats.completion_usage_count;
    }
    if (usage.total_tokens) {
        stats.total_tokens += *usage.total_tokens;
        ++stats.total_usage_count;
    }
    return status;
}

Status AIRequestExecutor::prepare_retry(HttpClient& client, const AIResource& config,
                                        QueryContext* query_context, Clock::time_point deadline,
                                        AIErrorType error, const Status& request_status,
                                        AIRequestStats& stats, bool collect_statistics) const {
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
    return wait_for_retry(delay_ms, query_context, deadline, stats, collect_statistics);
}

Status AIRequestExecutor::execute(const std::string& request_body, std::string& response,
                                  const AIResource& config, const AIAdapter& adapter,
                                  QueryContext* query_context, AIRequestStats* output_stats) {
    AIRequestStats stats;
    const bool collect_statistics = _statistics != nullptr || output_stats != nullptr;
    const Status result = execute_with_retry(request_body, response, config, adapter, query_context,
                                             stats, collect_statistics);
    publish_stats(stats, result, query_context, output_stats);
    return result;
}

Status AIRequestExecutor::execute_with_retry(const std::string& request_body, std::string& response,
                                             const AIResource& config, const AIAdapter& adapter,
                                             QueryContext* query_context, AIRequestStats& stats,
                                             bool collect_statistics) {
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
        Status status =
                execute_single_request(client.get(), request_body, response, config, adapter,
                                       query_context, deadline, stats, collect_statistics);
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
        RETURN_IF_ERROR(prepare_retry(*client, config, query_context, deadline, error, status,
                                      stats, collect_statistics));
    }
}

void AIRequestExecutor::publish_stats(const AIRequestStats& stats, const Status& status,
                                      QueryContext* query_context,
                                      AIRequestStats* output_stats) const {
    if (output_stats != nullptr) {
        *output_stats = stats;
    }
    if (_statistics) {
        _statistics->record_request(stats, status,
                                    query_context != nullptr && query_context->is_cancelled());
    }
}

} // namespace doris
