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
#include <google/cloud/internal/oauth2_decorate_credentials.h>
#include <google/cloud/internal/oauth2_error_credentials.h>
#include <google/cloud/internal/oauth2_google_application_default_credentials_file.h>
#include <google/cloud/internal/oauth2_service_account_credentials.h>
#include <google/cloud/internal/rest_client.h>
#include <google/cloud/internal/unified_rest_credentials.h>
#include <rapidjson/document.h>

#include <fstream>
#include <iterator>
#include <set>
#include <string_view>
#include <utility>

namespace doris {
namespace {

constexpr char GCS_READ_WRITE_SCOPE[] = "https://www.googleapis.com/auth/devstorage.read_write";
constexpr char CLOUD_PLATFORM_SCOPE[] = "https://www.googleapis.com/auth/cloud-platform";

std::shared_ptr<google::cloud::oauth2_internal::Credentials> make_default_storage_credentials(
        const google::cloud::Options& options) {
    namespace oauth2 = google::cloud::oauth2_internal;
    auto path = oauth2::GoogleAdcFilePathFromEnvVarOrEmpty();
    if (path.empty()) {
        path = oauth2::GoogleAdcFilePathFromWellKnownPathOrEmpty();
    }
    std::ifstream input(path);
    std::string contents(std::istreambuf_iterator<char> {input}, {});
    rapidjson::Document document;
    document.Parse(contents.data(), contents.size());
    // The generic SDK adapter emits scoped self-signed JWTs for service-account
    // JSON keys, which GCS rejects. Keep other ADC types (and their errors) on
    // the SDK's normal resolution path, including federation and VM identities.
    if (document.HasParseError() || !document.IsObject() || !document.HasMember("type") ||
        !document["type"].IsString() ||
        std::string_view(document["type"].GetString(), document["type"].GetStringLength()) !=
                "service_account") {
        return google::cloud::rest_internal::MapCredentials(
                *google::cloud::MakeGoogleDefaultCredentials(options));
    }
    auto info = oauth2::ParseServiceAccountCredentials(contents, path);
    if (!info) {
        return std::make_shared<oauth2::ErrorCredentials>(info.status());
    }
    info->enable_self_signed_jwt = false;
    const auto& scopes = options.get<google::cloud::ScopesOption>();
    info->scopes = std::set<std::string>(scopes.begin(), scopes.end());
    return oauth2::Decorate(std::make_shared<oauth2::ServiceAccountCredentials>(
                                    *info, options,
                                    [](const google::cloud::Options& opts) {
                                        return google::cloud::rest_internal::MakeDefaultRestClient(
                                                "", opts);
                                    }),
                            options);
}

std::shared_ptr<google::cloud::oauth2_internal::Credentials> make_credentials(
        const GcpCredentialConfig& credential, const std::string& ca_cert_path,
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
        // A direct service-account ADC token goes to GCS. Impersonation sources
        // instead authenticate to IAM, which accepts the SDK's self-signed JWTs.
        if (credential.impersonation_service_account.empty()) {
            return make_default_storage_credentials(options);
        }
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
    return google::cloud::rest_internal::MapCredentials(*credentials);
}

} // namespace

GcpTokenProvider::GcpTokenProvider(const GcpCredentialConfig& credential,
                                   const std::string& ca_cert_path, GcpTokenScope token_scope)
        : GcpTokenProvider(make_credentials(credential, ca_cert_path, token_scope)) {}

GcpTokenProvider::GcpTokenProvider(
        std::shared_ptr<google::cloud::oauth2_internal::Credentials> credentials)
        : _credentials(std::move(credentials)) {}

GcpTokenProvider::~GcpTokenProvider() = default;

std::optional<std::string> GcpTokenProvider::get_token() const {
    // GcpS3Client and presign callers may share one provider across request
    // threads. Serialize access even if the underlying implementation changes
    // its thread-safety guarantees in a future google-cloud-cpp release.
    std::lock_guard lock(_credentials_mutex);
    auto token = _credentials->GetToken(std::chrono::system_clock::now());
    if (!token) {
        LOG_EVERY_N(WARNING, 100) << "failed to obtain GCP access token: "
                                  << token.status().message();
        return std::nullopt;
    }
    return token->token;
}

std::string GcpTokenProvider::get_service_account_email() const {
    std::lock_guard lock(_credentials_mutex);
    return _credentials->AccountEmail();
}

} // namespace doris
