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
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>

namespace doris {

// Provides short-lived Google OAuth2 access tokens from the GCE/GKE metadata
// server. On GKE, the metadata server exchanges the pod's Kubernetes service
// account identity through Workload Identity Federation.
class GcpWorkloadIdentityTokenProvider {
public:
    using Clock = std::function<std::chrono::steady_clock::time_point()>;
    using TokenFetcher = std::function<bool(std::string* token, std::chrono::seconds* expires_in)>;

    GcpWorkloadIdentityTokenProvider();
    GcpWorkloadIdentityTokenProvider(TokenFetcher fetcher, Clock clock);

    GcpWorkloadIdentityTokenProvider(const GcpWorkloadIdentityTokenProvider&) = delete;
    GcpWorkloadIdentityTokenProvider& operator=(const GcpWorkloadIdentityTokenProvider&) = delete;

    // Returns a valid token, or an empty string when the metadata server cannot
    // provide one. A still-valid cached token is retained across transient
    // refresh failures.
    std::string get_token();

private:
    TokenFetcher _fetcher;
    Clock _clock;
    std::mutex _mutex;
    std::condition_variable _refresh_complete;
    bool _refresh_in_progress = false;
    std::string _cached_token;
    std::chrono::steady_clock::time_point _expire_at {};
    std::chrono::steady_clock::time_point _next_refresh_attempt {};
};

// Workload identity is process-wide, so all GCP vaults in one BE or recycler
// process share the same cached token.
std::shared_ptr<GcpWorkloadIdentityTokenProvider> global_gcp_workload_identity_token_provider();

template <typename Request>
void apply_gcp_bearer_token(
        Request& request, const std::shared_ptr<GcpWorkloadIdentityTokenProvider>& token_provider) {
    if (token_provider == nullptr) {
        return;
    }
    auto token = token_provider->get_token();
    if (token.empty()) {
        // Fail closed instead of silently falling back to anonymous access.
        request.SetAdditionalCustomHeaderValue("Authorization", "Bearer unavailable");
        return;
    }
    request.SetAdditionalCustomHeaderValue("Authorization", "Bearer " + token);
}

} // namespace doris
