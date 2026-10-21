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

#include <gtest/gtest.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_headers.h"
#include "service/http/http_request.h"
#include "service/http/http_status.h"

namespace doris {
namespace {

class OAuthTokenHandler : public HttpHandler {
public:
    void handle(HttpRequest* request) override {
        std::lock_guard lock(mutex);
        requests.push_back(request->get_request_body());
        request->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
        if (reject) {
            HttpChannel::send_reply(request, HttpStatus::UNAUTHORIZED,
                                    R"({"error":"invalid_grant"})");
            return;
        }
        HttpChannel::send_reply(
                request,
                R"({"access_token":"oauth-access-token","expires_in":3600,"token_type":"Bearer"})");
    }

    std::mutex mutex;
    std::vector<std::string> requests;
    bool reject = false;
};

class GcpTokenProviderTest : public testing::Test {
protected:
    void SetUp() override {
        for (size_t i = 0; i < ENV_NAMES.size(); ++i) {
            const char* value = std::getenv(ENV_NAMES[i]);
            saved_env[i] = value == nullptr ? std::nullopt : std::optional<std::string>(value);
            unsetenv(ENV_NAMES[i]);
        }
        char path[] = "/tmp/doris_gcp_adc_XXXXXX";
        int fd = mkstemp(path);
        ASSERT_GE(fd, 0);
        close(fd);
        adc_path = path;
        ASSERT_EQ(setenv("GOOGLE_APPLICATION_CREDENTIALS", adc_path.c_str(), 1), 0);
        ASSERT_TRUE(server.register_handler(POST, "/token", &handler));
        server.start();
        ASSERT_GT(server.get_real_port(), 0);
        token_uri = "http://127.0.0.1:" + std::to_string(server.get_real_port()) + "/token";

        // This is a freshly generated test key, never a real Google identity or secret.
        std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)> context(
                EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), EVP_PKEY_CTX_free);
        ASSERT_NE(context, nullptr);
        ASSERT_EQ(EVP_PKEY_keygen_init(context.get()), 1);
        ASSERT_EQ(EVP_PKEY_CTX_set_rsa_keygen_bits(context.get(), 2048), 1);
        EVP_PKEY* raw_key = nullptr;
        ASSERT_EQ(EVP_PKEY_keygen(context.get(), &raw_key), 1);
        std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(raw_key, EVP_PKEY_free);
        std::unique_ptr<BIO, decltype(&BIO_free)> bio(BIO_new(BIO_s_mem()), BIO_free);
        ASSERT_NE(bio, nullptr);
        ASSERT_EQ(PEM_write_bio_PrivateKey(bio.get(), key.get(), nullptr, nullptr, 0, nullptr,
                                           nullptr),
                  1);
        char* pem = nullptr;
        auto length = BIO_get_mem_data(bio.get(), &pem);
        private_key.assign(pem, length);
        write_service_account();
    }

    void TearDown() override {
        std::remove(adc_path.c_str());
        for (size_t i = 0; i < ENV_NAMES.size(); ++i) {
            if (saved_env[i]) {
                setenv(ENV_NAMES[i], saved_env[i]->c_str(), 1);
            } else {
                unsetenv(ENV_NAMES[i]);
            }
        }
    }

    void write_service_account() {
        rapidjson::Document document;
        document.SetObject();
        auto& allocator = document.GetAllocator();
        document.AddMember("type", "service_account", allocator);
        document.AddMember("client_email", "test@example.iam.gserviceaccount.com", allocator);
        document.AddMember("private_key_id", "test-key-id", allocator);
        document.AddMember("private_key", rapidjson::Value(private_key.c_str(), allocator),
                           allocator);
        document.AddMember("token_uri", rapidjson::Value(token_uri.c_str(), allocator), allocator);
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        document.Accept(writer);
        std::ofstream output(adc_path);
        output << buffer.GetString();
        ASSERT_TRUE(output.good());
    }

    void expect_exchange_scope(const std::string& expected_scope) {
        std::lock_guard lock(handler.mutex);
        ASSERT_EQ(handler.requests.size(), 1);
        const auto& body = handler.requests.front();
        EXPECT_NE(body.find("grant_type="), std::string::npos);
        auto assertion = body.find("assertion=");
        ASSERT_NE(assertion, std::string::npos);
        auto start = body.find('.', assertion);
        ASSERT_NE(start, std::string::npos);
        auto end = body.find('.', start + 1);
        ASSERT_NE(end, std::string::npos);
        auto encoded = body.substr(start + 1, end - start - 1);
        std::replace(encoded.begin(), encoded.end(), '-', '+');
        std::replace(encoded.begin(), encoded.end(), '_', '/');
        while (encoded.size() % 4 != 0) {
            encoded.push_back('=');
        }
        std::string decoded(encoded.size(), '\0');
        int length = EVP_DecodeBlock(reinterpret_cast<unsigned char*>(decoded.data()),
                                     reinterpret_cast<const unsigned char*>(encoded.data()),
                                     encoded.size());
        ASSERT_GT(length, 0);
        rapidjson::Document payload;
        payload.Parse(decoded.c_str());
        ASSERT_FALSE(payload.HasParseError());
        ASSERT_TRUE(payload.HasMember("scope"));
        EXPECT_EQ(std::string(payload["scope"].GetString()), expected_scope);
        ASSERT_TRUE(payload.HasMember("aud"));
        EXPECT_EQ(std::string(payload["aud"].GetString()), token_uri);
    }

    static constexpr std::array<const char*, 3> ENV_NAMES {
            "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_GCLOUD_ADC_PATH_OVERRIDE",
            "GOOGLE_CLOUD_CPP_EXPERIMENTAL_DISABLE_SELF_SIGNED_JWT"};
    std::array<std::optional<std::string>, ENV_NAMES.size()> saved_env;
    std::string adc_path;
    std::string private_key;
    std::string token_uri;
    OAuthTokenHandler handler;
    EvHttpServer server {"127.0.0.1", 0};
};

TEST_F(GcpTokenProviderTest, ServiceAccountAdcExchangesAndCachesOAuthToken) {
    GcpTokenProvider provider(GcpCredentialConfig {});
    EXPECT_EQ(provider.get_token(), "oauth-access-token");
    EXPECT_EQ(provider.get_token(), "oauth-access-token");
    EXPECT_EQ(provider.get_service_account_email(), "test@example.iam.gserviceaccount.com");
    expect_exchange_scope("https://www.googleapis.com/auth/devstorage.read_write");
    EXPECT_EQ(std::getenv("GOOGLE_CLOUD_CPP_EXPERIMENTAL_DISABLE_SELF_SIGNED_JWT"), nullptr);
}

TEST_F(GcpTokenProviderTest, WellKnownAdcExchangesOAuthTokenWithCloudPlatformScope) {
    unsetenv("GOOGLE_APPLICATION_CREDENTIALS");
    ASSERT_EQ(setenv("GOOGLE_GCLOUD_ADC_PATH_OVERRIDE", adc_path.c_str(), 1), 0);
    GcpTokenProvider provider(GcpCredentialConfig {}, "", GcpTokenScope::CloudPlatform);
    EXPECT_EQ(provider.get_token(), "oauth-access-token");
    expect_exchange_scope("https://www.googleapis.com/auth/cloud-platform");
}

TEST_F(GcpTokenProviderTest, OAuthFailureDoesNotReturnSelfSignedJwt) {
    {
        std::lock_guard lock(handler.mutex);
        handler.reject = true;
    }
    GcpTokenProvider provider(GcpCredentialConfig {});
    EXPECT_FALSE(provider.get_token().has_value());
    expect_exchange_scope("https://www.googleapis.com/auth/devstorage.read_write");
}

TEST_F(GcpTokenProviderTest, InvalidServiceAccountAdcDoesNotFallBackToAnotherIdentity) {
    std::ofstream(adc_path)
            << R"({"type":"service_account","client_email":"test@example.iam.gserviceaccount.com"})";
    GcpTokenProvider provider(GcpCredentialConfig {});
    EXPECT_FALSE(provider.get_token().has_value());
    std::lock_guard lock(handler.mutex);
    EXPECT_TRUE(handler.requests.empty());
}

} // namespace
} // namespace doris
