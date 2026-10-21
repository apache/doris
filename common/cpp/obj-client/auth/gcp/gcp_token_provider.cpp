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

#include "cpp/obj-client/auth/gcp/gcp_token_provider.h"

#include <glog/logging.h>
#include <google/cloud/credentials.h>
#include <google/cloud/oauth2/access_token_generator.h>

#include <utility>

namespace doris {
namespace {

constexpr char GCS_READ_WRITE_SCOPE[] = "https://www.googleapis.com/auth/devstorage.read_write";
constexpr char CLOUD_PLATFORM_SCOPE[] = "https://www.googleapis.com/auth/cloud-platform";

std::shared_ptr<google::cloud::Credentials> make_credentials(const GcpCredentialConfig& credential,
                                                             const std::string& ca_cert_path,
                                                             GcpTokenScope token_scope) {
    google::cloud::Options options;
    if (!ca_cert_path.empty()) {
        options.set<google::cloud::CARootsFilePathOption>(ca_cert_path);
    }

    // A source credential used for impersonation calls the IAM Credentials API
    // and therefore needs cloud-platform. The resulting target token uses the
    // caller-selected scope: object read/write for storage requests, or
    // cloud-platform when it will call IAM signBlob.
    const char* requested_scope = token_scope == GcpTokenScope::CloudPlatform
                                          ? CLOUD_PLATFORM_SCOPE
                                          : GCS_READ_WRITE_SCOPE;
    options.set<google::cloud::ScopesOption>({credential.impersonation_service_account.empty()
                                                      ? requested_scope
                                                      : CLOUD_PLATFORM_SCOPE});

    std::shared_ptr<google::cloud::Credentials> credentials;
    switch (credential.provider_type) {
    case GcpCredentialProviderType::Default:
        credentials = google::cloud::MakeGoogleDefaultCredentials(options);
        break;
    case GcpCredentialProviderType::ComputeEngine:
        credentials = google::cloud::MakeComputeEngineCredentials(options);
        break;
    }
    if (!credential.impersonation_service_account.empty()) {
        auto impersonation_options = options;
        impersonation_options.set<google::cloud::ScopesOption>({requested_scope});
        credentials = google::cloud::MakeImpersonateServiceAccountCredentials(
                std::move(credentials), credential.impersonation_service_account,
                std::move(impersonation_options));
    }
    return credentials;
}

} // namespace

GcpTokenProvider::GcpTokenProvider(const GcpCredentialConfig& credential,
                                   const std::string& ca_cert_path, GcpTokenScope token_scope)
        : _credentials(make_credentials(credential, ca_cert_path, token_scope)),
          _token_generator(google::cloud::oauth2::MakeAccessTokenGenerator(*_credentials)) {}

GcpTokenProvider::~GcpTokenProvider() = default;

std::optional<std::string> GcpTokenProvider::get_token() const {
    // GcpS3Client and presign callers may share one provider across request
    // threads. Serialize access even if the underlying implementation changes
    // its thread-safety guarantees in a future google-cloud-cpp release.
    std::lock_guard lock(_token_generator_mutex);
    auto token = _token_generator->GetToken();
    if (!token) {
        LOG_EVERY_N(WARNING, 100) << "failed to obtain GCP access token: "
                                  << token.status().message();
        return std::nullopt;
    }
    return token->token;
}

} // namespace doris
