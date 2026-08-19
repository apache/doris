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

#include "gcp_workload_identity_token_provider.h"

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/HttpClientFactory.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/stream/ResponseStream.h>
#include <glog/logging.h>

#include <sstream>
#include <string_view>
#include <utility>

namespace doris {
namespace {

constexpr std::chrono::seconds REFRESH_MARGIN {300};
constexpr std::chrono::seconds REFRESH_RETRY_DELAY {30};
constexpr std::string_view METADATA_TOKEN_URL =
        "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

Aws::Client::ClientConfiguration http_client_configuration() {
    Aws::Client::ClientConfiguration config(false, "legacy", true);
    config.connectTimeoutMs = 2000;
    config.requestTimeoutMs = 5000;
    config.followRedirects = Aws::Client::FollowRedirectsPolicy::NEVER;
    config.allowSystemProxy = false;
    config.proxyHost.clear();
    config.proxyUserName.clear();
    config.proxyPassword.clear();
    config.proxyPort = 0;
    return config;
}

bool fetch_from_metadata_server(std::string* token, std::chrono::seconds* expires_in) {
    auto request =
            Aws::Http::CreateHttpRequest(Aws::String(METADATA_TOKEN_URL.data()),
                                         Aws::Http::HttpMethod::HTTP_GET,
                                         Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);
    request->SetHeaderValue("Metadata-Flavor", "Google");

    auto client = Aws::Http::CreateHttpClient(http_client_configuration());
    auto response = client->MakeRequest(request);
    if (response == nullptr || response->GetResponseCode() != Aws::Http::HttpResponseCode::OK) {
        LOG(WARNING) << "GCP workload identity: metadata token request failed, http_code="
                     << (response == nullptr ? -1 : static_cast<int>(response->GetResponseCode()));
        return false;
    }

    std::stringstream body;
    body << response->GetResponseBody().rdbuf();
    Aws::Utils::Json::JsonValue json(Aws::String(body.str()));
    if (!json.WasParseSuccessful()) {
        LOG(WARNING) << "GCP workload identity: metadata server returned invalid JSON";
        return false;
    }

    auto view = json.View();
    if (!view.ValueExists("access_token") || !view.ValueExists("expires_in")) {
        LOG(WARNING) << "GCP workload identity: metadata response is missing access_token or "
                        "expires_in";
        return false;
    }

    *token = view.GetString("access_token");
    *expires_in = std::chrono::seconds(view.GetInteger("expires_in"));
    if (token->empty() || *expires_in <= std::chrono::seconds::zero()) {
        LOG(WARNING) << "GCP workload identity: metadata server returned an invalid token";
        return false;
    }
    return true;
}

} // namespace

GcpWorkloadIdentityTokenProvider::GcpWorkloadIdentityTokenProvider()
        : GcpWorkloadIdentityTokenProvider(fetch_from_metadata_server,
                                           std::chrono::steady_clock::now) {}

GcpWorkloadIdentityTokenProvider::GcpWorkloadIdentityTokenProvider(TokenFetcher fetcher,
                                                                   Clock clock)
        : _fetcher(std::move(fetcher)), _clock(std::move(clock)) {}

std::string GcpWorkloadIdentityTokenProvider::get_token() {
    std::unique_lock lock(_mutex);
    while (true) {
        auto now = _clock();
        bool has_valid_token = !_cached_token.empty() && now < _expire_at;
        if (has_valid_token && now + REFRESH_MARGIN < _expire_at) {
            return _cached_token;
        }
        if (now < _next_refresh_attempt) {
            return has_valid_token ? _cached_token : "";
        }
        if (!_refresh_in_progress) {
            _refresh_in_progress = true;
            break;
        }
        if (has_valid_token) {
            return _cached_token;
        }
        _refresh_complete.wait(lock, [this] { return !_refresh_in_progress; });
    }

    std::string token;
    std::chrono::seconds expires_in {0};
    lock.unlock();
    bool refreshed = _fetcher(&token, &expires_in) && !token.empty() &&
                     expires_in > std::chrono::seconds::zero();
    lock.lock();

    auto now = _clock();
    if (refreshed) {
        _cached_token = std::move(token);
        _expire_at = now + expires_in;
        _next_refresh_attempt = {};
    } else {
        _next_refresh_attempt = now + REFRESH_RETRY_DELAY;
    }
    _refresh_in_progress = false;
    _refresh_complete.notify_all();

    if (!refreshed) {
        if (!_cached_token.empty() && now < _expire_at) {
            return _cached_token;
        }
        LOG(WARNING) << "GCP workload identity: no valid access token is available";
        return "";
    }
    return _cached_token;
}

std::shared_ptr<GcpWorkloadIdentityTokenProvider> global_gcp_workload_identity_token_provider() {
    static auto provider = std::make_shared<GcpWorkloadIdentityTokenProvider>();
    return provider;
}

} // namespace doris
