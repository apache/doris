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
#include <memory>
#include <mutex>
#include <string>

namespace doris {

// Provides OAuth2 access tokens for Google Cloud Storage following the
// Application Default Credentials (ADC) resolution order:
//   1. A service account json file pointed to by the
//      GOOGLE_APPLICATION_CREDENTIALS environment variable (a signed JWT is
//      exchanged for an access token at the OAuth2 token endpoint).
//   2. The GCE/GKE metadata server (which serves the identity attached to
//      the instance or, on GKE with Workload Identity, to the pod).
//
// Tokens are cached and refreshed ahead of expiry. Thread-safe.
//
// Used by the GCP_ADC storage vault credentials provider type: the S3 client
// talking to the GCS S3-compatible XML API attaches the token as an
// `Authorization: Bearer` header instead of signing with HMAC keys.
class GcpAdcTokenProvider {
public:
    GcpAdcTokenProvider() = default;
    virtual ~GcpAdcTokenProvider() = default;

    GcpAdcTokenProvider(const GcpAdcTokenProvider&) = delete;
    GcpAdcTokenProvider& operator=(const GcpAdcTokenProvider&) = delete;

    // Returns a valid access token, or an empty string if none of the ADC
    // sources could provide one (the failure is logged).
    std::string get_token();

protected:
    // Fetch a fresh {access_token, expires_in} pair from the first available
    // ADC source. Returns true on success. Virtual for tests.
    virtual bool refresh_token(std::string* token, std::chrono::seconds* expires_in);

    // Individual sources, virtual for tests.
    virtual bool fetch_from_service_account_file(const std::string& credentials_file,
                                                 std::string* token,
                                                 std::chrono::seconds* expires_in);
    virtual bool fetch_from_metadata_server(std::string* token, std::chrono::seconds* expires_in);

private:
    std::mutex _mutex;
    std::string _cached_token;
    std::chrono::steady_clock::time_point _expire_at {};
};

// The provider is shared by every client of the same process (BE or
// recycler); ADC resolves process-wide state (env + metadata server), so a
// single cached token is correct for all vaults using GCP_ADC.
std::shared_ptr<GcpAdcTokenProvider> global_gcp_adc_token_provider();

// Attach an `Authorization: Bearer` header to an AWS SDK request when a
// provider is configured; no-op otherwise. The paired S3 client is created
// with anonymous credentials, so the SDK's SigV4 signer leaves the header
// untouched and GCS authenticates the request with the OAuth2 token.
template <typename Request>
void apply_gcp_bearer_token(Request& request,
                            const std::shared_ptr<GcpAdcTokenProvider>& provider) {
    if (provider != nullptr) {
        request.SetAdditionalCustomHeaderValue("Authorization", "Bearer " + provider->get_token());
    }
}

} // namespace doris
