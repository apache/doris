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

#include "cpp/obj-storage/auth/gcp/gcs_signed_url_provider.h"

#include <curl/curl.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <utility>

#include "cpp/obj-storage/auth/gcp/gcp_token_provider.h"
#include "cpp/obj-storage/client/gcp_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "io/object_storage/be_obj_storage_config_parser.h"

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
        SyncPoint::get_instance()->clear_call_back("GcsV4Signer::set_option");
        SyncPoint::get_instance()->clear_call_back("GcsV4Signer::token_provider");
        SyncPoint::get_instance()->disable_processing();
    }

    void check_signing_timeout(const ObjStorageResourceConfig& resource, long expected_ms) {
        SCOPED_TRACE(expected_ms);
        auto* sync = SyncPoint::get_instance();
        sync->set_call_back("GcsV4Signer::token_provider", [](std::vector<std::any>&& args) {
            *try_any_cast<std::shared_ptr<GcpTokenProvider>*>(args[0]) =
                    std::make_shared<GcpTokenProvider>(std::make_shared<SelectedAccountCredentials>(
                            "vm@my-project.iam.gserviceaccount.com"));
        });
        bool timeout_set = false;
        sync->set_call_back("GcsV4Signer::set_option", [&](std::vector<std::any>&& args) {
            if (try_any_cast<CURLoption>(args[0]) == CURLOPT_TIMEOUT_MS) {
                timeout_set = true;
                EXPECT_EQ(try_any_cast<long>(args[1]), expected_ms);
            }
        });
        sync->set_call_back("GcsV4Signer::sign_blob", [](std::vector<std::any>&& args) {
            *try_any_cast<std::string*>(args[3]) = "test-signature";
            try_any_cast_ret<google::cloud::Status>(args)->second = true;
        });
        sync->enable_processing();
        auto client = GcpObjStorageClient::create(std::get<GcpObjStorageConfig>(resource.client));
        ASSERT_TRUE(client.client) << client.error;
        EXPECT_FALSE(
                client.client
                        ->generate_presigned_url(
                                {.bucket = resource.bucket, .key = "load-errors/error.log"}, 300)
                        .empty());
        EXPECT_TRUE(timeout_set);
        sync->clear_call_back("GcsV4Signer::set_option");
    }
};

TEST_F(GcsSignedUrlProviderTest, NativeGcpSigningTimeoutIsIndependentOfObjectIoDefaults) {
    std::map<std::string, std::string> properties {
            {"provider", "GCP"},
            {"AWS_ENDPOINT", "https://storage.googleapis.com"},
            {"AWS_REGION", "us-east1"},
            {"gs.credential_provider_type", "COMPUTE_ENGINE"}};
    TS3StorageParam thrift;
    thrift.__set_provider(TObjStorageType::GCP);
    thrift.__set_bucket("bucket");
    thrift.__set_endpoint("https://storage.googleapis.com");
    thrift.__set_region("us-east1");
    TGcpCredential gcp;
    gcp.__set_credential_provider_type(TGcpCredentialProviderType::COMPUTE_ENGINE);
    TCredential credential;
    credential.__set_gcp_credential(gcp);
    thrift.__set_credential(credential);
    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB::GCP);
    vault.set_bucket("bucket");
    vault.set_endpoint("https://storage.googleapis.com");
    vault.set_region("us-east1");
    vault.mutable_credential()->mutable_gcp_credential();

    for (const auto& parsed :
         {parse_be_obj_storage_config(properties, "bucket"), parse_be_obj_storage_config(vault)}) {
        ASSERT_TRUE(parsed.has_value()) << parsed.error();
        EXPECT_EQ(obj_storage_connection(parsed->client).request_timeout, std::chrono::seconds(30));
        check_signing_timeout(*parsed, 10000);
    }
    // An omitted Thrift field retains the protocol's positive 3000ms default.
    auto omitted = parse_be_obj_storage_config(thrift);
    ASSERT_TRUE(omitted.has_value()) << omitted.error();
    check_signing_timeout(*omitted, 3000);
    for (int timeout_ms : {-1, 0, 2345, 30000}) {
        properties["AWS_REQUEST_TIMEOUT_MS"] = std::to_string(timeout_ms);
        thrift.__set_request_timeout_ms(timeout_ms);
        for (const auto& parsed : {parse_be_obj_storage_config(properties, "bucket"),
                                   parse_be_obj_storage_config(thrift)}) {
            ASSERT_TRUE(parsed.has_value()) << parsed.error();
            EXPECT_EQ(obj_storage_connection(parsed->client).request_timeout,
                      std::chrono::milliseconds(timeout_ms > 0 ? timeout_ms : 30000));
            check_signing_timeout(*parsed, timeout_ms > 0 ? timeout_ms : 10000);
        }
    }
}

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
                auto* result = try_any_cast_ret<google::cloud::Status>(args);
                result->second = true;
            });
    SyncPoint::get_instance()->enable_processing();
    GcpCredentialConfig credential;
    credential.provider_type = GcpCredentialProviderType::Default;
    GcsV4SignedUrlProviderOptions options {.endpoint = "storage.googleapis.com",
                                           .bucket = "bucket",
                                           .key = "load-errors/error.log",
                                           .expiration_secs = 300,
                                           .ca_cert_file_path = ""};
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
    bool ca_set = false;
    SyncPoint::get_instance()->set_call_back(
            "GcsV4Signer::set_option", [&](std::vector<std::any>&& args) {
                if (try_any_cast<CURLoption>(args[0]) == CURLOPT_CAINFO) {
                    ca_set = true;
                    EXPECT_STREQ(try_any_cast<const char*>(args[1]),
                                 options.ca_cert_file_path.c_str());
                }
            });
    SyncPoint::get_instance()->set_call_back(
            "GcsV4Signer::sign_blob", [&](std::vector<std::any>&& args) {
                called = true;
                *try_any_cast<std::string*>(args[3]) = "test-signature";
                try_any_cast_ret<google::cloud::Status>(args)->second = true;
            });
    SyncPoint::get_instance()->enable_processing();
    std::string url;
    auto status = generate_gcs_v4_signed_url(options, GcpCredentialConfig {}, tokens, &url);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(called);
    EXPECT_TRUE(ca_set);
    EXPECT_NE(url.find("X-Goog-Signature="), std::string::npos);
}

TEST_F(GcsSignedUrlProviderTest, SignBlobRejectsCaOptionFailureBeforeSending) {
    auto tokens = std::make_shared<GcpTokenProvider>(
            std::make_shared<SelectedAccountCredentials>("vm@my-project.iam.gserviceaccount.com"));
    GcsV4SignedUrlProviderOptions options {.endpoint = "storage.googleapis.com",
                                           .bucket = "bucket",
                                           .key = "load-errors/error.log",
                                           .expiration_secs = 300,
                                           .ca_cert_file_path = "/custom/certs/proxy-ca.pem"};
    bool ca_set = false;
    bool sent = false;
    SyncPoint::get_instance()->set_call_back(
            "GcsV4Signer::set_option", [&](std::vector<std::any>&& args) {
                if (try_any_cast<CURLoption>(args[0]) == CURLOPT_CAINFO) {
                    ca_set = true;
                    auto* result = try_any_cast_ret<google::cloud::Status>(args);
                    result->first = google::cloud::Status(
                            google::cloud::StatusCode::kInvalidArgument, "CA option rejected");
                    result->second = true;
                }
            });
    SyncPoint::get_instance()->set_call_back(
            "GcsV4Signer::sign_blob", [&](std::vector<std::any>&& args) {
                sent = true;
                try_any_cast_ret<google::cloud::Status>(args)->second = true;
            });
    SyncPoint::get_instance()->enable_processing();
    std::string url;
    auto status = generate_gcs_v4_signed_url(options, GcpCredentialConfig {}, tokens, &url);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.message().find("CA option rejected"), std::string::npos);
    EXPECT_TRUE(ca_set);
    EXPECT_FALSE(sent);
    EXPECT_TRUE(url.empty());
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
