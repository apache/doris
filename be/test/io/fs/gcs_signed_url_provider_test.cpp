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

#include "io/fs/gcs_signed_url_provider.h"

#include <gtest/gtest.h>

#include <chrono>
#include <utility>

#include "cpp/obj-client/auth/gcp/gcp_token_provider.h"
#include "cpp/sync_point.h"

namespace doris::io {
namespace {

class SelectedAccountCredentials : public google::cloud::oauth2_internal::Credentials {
public:
    explicit SelectedAccountCredentials(std::string email) : _email(std::move(email)) {}

    google::cloud::StatusOr<google::cloud::AccessToken> GetToken(
            std::chrono::system_clock::time_point now) override {
        return google::cloud::AccessToken {"test-token", now + std::chrono::hours(1)};
    }

    std::string AccountEmail() const override { return _email; }

private:
    std::string _email;
};

class GcsSignedUrlProviderTest : public testing::Test {
protected:
    void TearDown() override {
        SyncPoint::get_instance()->clear_call_back("GcsV4Signer::sign_blob");
        SyncPoint::get_instance()->clear_call_back("HttpClient::set_ca_cert_file");
        SyncPoint::get_instance()->disable_processing();
    }
};

TEST_F(GcsSignedUrlProviderTest, DefaultSignsErrorLogWithResolvedIdentity) {
    auto tokens = std::make_shared<GcpTokenProvider>(
            std::make_shared<SelectedAccountCredentials>("vm@my-project.iam.gserviceaccount.com"));
    bool called = false;
    SyncPoint::get_instance()->set_call_back(
            "GcsV4Signer::sign_blob", [&](std::vector<std::any>&& args) {
                called = true;
                EXPECT_EQ(try_any_cast<std::string_view>(args[0]), "test-token");
                EXPECT_EQ(try_any_cast<std::string_view>(args[1]),
                          "vm@my-project.iam.gserviceaccount.com");
                EXPECT_EQ(try_any_cast<std::string_view>(args[2]).find("GOOG4-RSA-SHA256"), 0);
                *try_any_cast<std::string*>(args[3]) = "test-signature";
                auto* result = try_any_cast_ret<Status>(args);
                result->second = true;
            });
    SyncPoint::get_instance()->enable_processing();
    GcpCredentialConfig credential;
    credential.provider_type = GcpCredentialProviderType::Default;
    GcsV4SignedUrlProviderOptions options {.endpoint = "storage.googleapis.com",
                                           .bucket = "bucket",
                                           .key = "load-errors/error.log",
                                           .expiration_secs = 300};
    std::string url;
    auto status = generate_gcs_v4_signed_url(options, credential, tokens, &url);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(called);
    EXPECT_NE(url.find("vm%40my-project.iam.gserviceaccount.com"), std::string::npos);
    EXPECT_NE(url.find("X-Goog-Signature="), std::string::npos);
}

TEST_F(GcsSignedUrlProviderTest, SignBlobUsesConfiguredCaBundle) {
    auto tokens = std::make_shared<GcpTokenProvider>(
            std::make_shared<SelectedAccountCredentials>("vm@my-project.iam.gserviceaccount.com"));
    GcsV4SignedUrlProviderOptions options {.endpoint = "storage.googleapis.com",
                                           .bucket = "bucket",
                                           .key = "load-errors/error.log",
                                           .expiration_secs = 300,
                                           .ca_cert_file_path = "/custom/certs/proxy-ca.pem"};
    bool called = false;
    SyncPoint::get_instance()->set_call_back(
            "HttpClient::set_ca_cert_file", [&](std::vector<std::any>&& args) {
                called = true;
                EXPECT_EQ(*try_any_cast<const std::string*>(args[0]), options.ca_cert_file_path);
            });
    SyncPoint::get_instance()->set_call_back(
            "GcsV4Signer::sign_blob", [&](std::vector<std::any>&& args) {
                // Exercise HTTP client setup but avoid sending a live IAM request.
                *try_any_cast<std::string*>(args[3]) = "test-signature";
                try_any_cast_ret<Status>(args)->second = true;
            });
    SyncPoint::get_instance()->enable_processing();
    std::string url;
    auto status = generate_gcs_v4_signed_url(options, GcpCredentialConfig {}, tokens, &url);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(called);
    EXPECT_NE(url.find("X-Goog-Signature="), std::string::npos);
}

TEST_F(GcsSignedUrlProviderTest, DefaultAndComputeUseTheSelectedAccount) {
    // Represents ADC selecting either the VM account or a service-account file.
    // Both must use the account attached to the token, without a separate metadata lookup.
    for (auto source :
         {GcpCredentialProviderType::Default, GcpCredentialProviderType::ComputeEngine}) {
        for (const auto* email :
             {"vm@my-project.iam.gserviceaccount.com", "file@my-project.iam.gserviceaccount.com"}) {
            GcpTokenProvider tokens(std::make_shared<SelectedAccountCredentials>(email));
            EXPECT_EQ(tokens.get_token(), "test-token");
            GcpCredentialConfig credential;
            credential.provider_type = source;
            std::string signer;
            ASSERT_TRUE(resolve_gcs_signer_email(credential, tokens, &signer).ok());
            EXPECT_EQ(signer, email);
        }
    }
}

TEST_F(GcsSignedUrlProviderTest, AccountlessAdcRequiresExplicitImpersonation) {
    GcpTokenProvider tokens(std::make_shared<SelectedAccountCredentials>(""));
    GcpCredentialConfig credential;
    credential.provider_type = GcpCredentialProviderType::Default;
    std::string signer;
    EXPECT_FALSE(resolve_gcs_signer_email(credential, tokens, &signer).ok());

    credential.impersonation_service_account = "target@my-project.iam.gserviceaccount.com";
    ASSERT_TRUE(resolve_gcs_signer_email(credential, tokens, &signer).ok());
    EXPECT_EQ(signer, credential.impersonation_service_account);
}

TEST_F(GcsSignedUrlProviderTest, ImpersonationOverridesSourceAccount) {
    GcpTokenProvider tokens(std::make_shared<SelectedAccountCredentials>(
            "source@my-project.iam.gserviceaccount.com"));
    GcpCredentialConfig credential;
    credential.impersonation_service_account = "target@my-project.iam.gserviceaccount.com";
    std::string signer;
    ASSERT_TRUE(resolve_gcs_signer_email(credential, tokens, &signer).ok());
    EXPECT_EQ(signer, credential.impersonation_service_account);
}

} // namespace
} // namespace doris::io
