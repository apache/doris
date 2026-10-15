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

#include "io/obj_storage/obj_client_factory.h"

#include <gtest/gtest.h>

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <variant>

#include "cloud/config.h"
#include "common/config.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "io/fs/s3_file_system.h"
#include "io/obj_storage/obj_storage_config.h"
#include "io/obj_storage/obj_storage_config_resolver.h"
#include "util/s3_util.h"

namespace doris::io {
namespace {

class ObjClientSyncPointGuard {
public:
    ObjClientSyncPointGuard() : _was_enabled(SyncPoint::get_instance()->get_enable()) {
        SyncPoint::get_instance()->enable_processing();
    }
    ~ObjClientSyncPointGuard() {
        if (!_was_enabled) {
            SyncPoint::get_instance()->disable_processing();
        }
    }

private:
    bool _was_enabled;
};

S3ClientConf make_s3_conf(std::string endpoint) {
    S3ClientConf conf;
    conf.endpoint = std::move(endpoint);
    conf.region = "us-east-1";
    conf.ak = "ak";
    conf.sk = "sk";
    conf.bucket = "bucket";
    return conf;
}

S3ClientConf make_azure_conf(std::string endpoint) {
    auto conf = make_s3_conf(std::move(endpoint));
    conf.provider = ObjStorageProvider::AZURE;
    conf.ak = "account";
    conf.sk = "YWNjb3VudC1rZXk=";
    conf.bucket = "container";
    return conf;
}

} // namespace

class ObjClientFactoryTest : public testing::Test {
protected:
    void SetUp() override {
        // Stub the AWS SDK client: every S3 build returns a fresh S3ObjStorageClient, so
        // pointer equality tells whether a client came from the provider cache.
        _sync_point->set_call_back(
                "s3_client_factory::create",
                [this](auto&& args) {
                    ++_s3_builds;
                    if (_on_s3_build) {
                        // Run once: a nested build must not re-enter the hook.
                        auto on_build = std::exchange(_on_s3_build, nullptr);
                        on_build();
                    }
                    auto result = try_any_cast_ret<std::shared_ptr<S3ObjStorageClient>>(args);
                    result->second = true;
                },
                &_create_client_callback);
    }

    ObjClientSyncPointGuard _sync_point_guard;
    SyncPoint* _sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard _create_client_callback;
    int _s3_builds = 0;
    std::function<void()> _on_s3_build;
};

TEST_F(ObjClientFactoryTest, ResolvesS3ConfIntoS3Backend) {
    auto conf = make_s3_conf("resolve-s3.example.com");
    conf.provider = ObjStorageProvider::OSS;
    conf.token = "token";
    conf.role_arn = "role";
    conf.external_id = "external";
    conf.cred_provider_type = CredProviderType::InstanceProfile;
    conf.use_virtual_addressing = false;
    conf.need_override_endpoint = false;
    conf.max_connections = 10;
    conf.request_timeout_ms = 20;
    conf.connect_timeout_ms = 30;
    conf.is_internal_bucket = true;

    auto config = ObjStorageConfigResolver::from_client_conf(conf);

    ASSERT_TRUE(std::holds_alternative<S3ClientConfig>(config.backend));
    const auto& s3 = std::get<S3ClientConfig>(config.backend);
    EXPECT_EQ(s3.vendor, ObjStorageProvider::OSS);
    EXPECT_EQ(s3.endpoint, "resolve-s3.example.com");
    EXPECT_EQ(s3.region, "us-east-1");
    EXPECT_FALSE(s3.need_override_endpoint);
    EXPECT_FALSE(s3.use_virtual_addressing);
    EXPECT_EQ(s3.auth.access_key, "ak");
    EXPECT_EQ(s3.auth.secret_key, "sk");
    EXPECT_EQ(s3.auth.session_token, "token");
    EXPECT_EQ(s3.auth.provider_type, CredProviderType::InstanceProfile);
    EXPECT_EQ(s3.auth.role_arn, "role");
    EXPECT_EQ(s3.auth.external_id, "external");
    EXPECT_EQ(config.connection.max_connections, 10);
    EXPECT_EQ(config.connection.request_timeout_ms, 20);
    EXPECT_EQ(config.connection.connect_timeout_ms, 30);
    EXPECT_EQ(config.usage, ClientUsage::INTERNAL_STORAGE);
    EXPECT_EQ(config.provider(), ObjStorageProvider::OSS);
    EXPECT_EQ(config.endpoint(), "resolve-s3.example.com");
}

TEST_F(ObjClientFactoryTest, ResolvesAzureConfIntoAzureBackend) {
    auto conf = make_azure_conf("resolve-azure.example.com");

    auto config = ObjStorageConfigResolver::from_client_conf(conf);

    ASSERT_TRUE(std::holds_alternative<AzureClientConfig>(config.backend));
    const auto& azure = std::get<AzureClientConfig>(config.backend);
    EXPECT_EQ(azure.endpoint, "resolve-azure.example.com");
    EXPECT_EQ(azure.account, "account");
    EXPECT_EQ(azure.container, "container");
    ASSERT_TRUE(std::holds_alternative<AzureSharedKeyConfig>(azure.auth));
    EXPECT_EQ(std::get<AzureSharedKeyConfig>(azure.auth).account_key, "YWNjb3VudC1rZXk=");
    EXPECT_EQ(config.usage, ClientUsage::EXTERNAL_ACCESS);
    EXPECT_EQ(config.provider(), ObjStorageProvider::AZURE);
}

TEST_F(ObjClientFactoryTest, IdentityOnlyCoversFieldsOfTheSelectedBackend) {
    // Azure never read region, token or the AWS role, so they do not split Azure clients.
    auto azure_conf = make_azure_conf("identity-azure.example.com");
    auto azure_with_s3_fields = azure_conf;
    azure_with_s3_fields.region = "other-region";
    azure_with_s3_fields.token = "token";
    azure_with_s3_fields.role_arn = "role";
    EXPECT_EQ(ObjStorageConfigResolver::from_client_conf(azure_conf),
              ObjStorageConfigResolver::from_client_conf(azure_with_s3_fields));

    // The Azure client is bound to one container; an S3 client is not bound to a bucket.
    auto azure_other_container = azure_conf;
    azure_other_container.bucket = "other-container";
    EXPECT_NE(ObjStorageConfigResolver::from_client_conf(azure_conf),
              ObjStorageConfigResolver::from_client_conf(azure_other_container));

    auto s3_conf = make_s3_conf("identity-s3.example.com");
    auto s3_other_bucket = s3_conf;
    s3_other_bucket.bucket = "other-bucket";
    EXPECT_EQ(ObjStorageConfigResolver::from_client_conf(s3_conf),
              ObjStorageConfigResolver::from_client_conf(s3_other_bucket));

    auto s3_other_region = s3_conf;
    s3_other_region.region = "other-region";
    EXPECT_NE(ObjStorageConfigResolver::from_client_conf(s3_conf),
              ObjStorageConfigResolver::from_client_conf(s3_other_region));

    // The same endpoint and credentials on another backend or usage is another client.
    auto s3_as_azure = s3_conf;
    s3_as_azure.provider = ObjStorageProvider::AZURE;
    EXPECT_NE(ObjStorageConfigResolver::from_client_conf(s3_conf),
              ObjStorageConfigResolver::from_client_conf(s3_as_azure));
    auto s3_internal = s3_conf;
    s3_internal.is_internal_bucket = true;
    EXPECT_NE(ObjStorageConfigResolver::from_client_conf(s3_conf),
              ObjStorageConfigResolver::from_client_conf(s3_internal));
}

TEST_F(ObjClientFactoryTest, ToStringMasksCredentials) {
    auto s3_conf = make_s3_conf("mask-s3.example.com");
    s3_conf.ak = "AKIDEXAMPLE";
    s3_conf.sk = "s3-secret-key";
    s3_conf.token = "s3-session-token";
    auto s3_text = ObjStorageConfigResolver::from_client_conf(s3_conf).to_string();
    EXPECT_EQ(s3_text.find("AKIDEXAMPLE"), std::string::npos);
    EXPECT_EQ(s3_text.find("s3-secret-key"), std::string::npos);
    EXPECT_EQ(s3_text.find("s3-session-token"), std::string::npos);

    auto azure_text =
            ObjStorageConfigResolver::from_client_conf(make_azure_conf("mask-azure.example.com"))
                    .to_string();
    EXPECT_EQ(azure_text.find("YWNjb3VudC1rZXk="), std::string::npos);
    EXPECT_NE(azure_text.find("shared_key"), std::string::npos);
}

TEST_F(ObjClientFactoryTest, S3ProviderCachesByFullConfig) {
    auto& factory = ObjClientFactory::instance();
    auto conf = make_s3_conf("s3-provider-cache.example.com");

    auto first = factory.get_or_create(ObjStorageConfigResolver::from_client_conf(conf));
    ASSERT_TRUE(first.has_value()) << first.error();
    EXPECT_EQ(_s3_builds, 1);

    auto other_bucket = conf;
    other_bucket.bucket = "other-bucket";
    auto shared = factory.get_or_create(ObjStorageConfigResolver::from_client_conf(other_bucket));
    ASSERT_TRUE(shared.has_value()) << shared.error();
    EXPECT_EQ(shared.value(), first.value());
    EXPECT_EQ(_s3_builds, 1);

    auto other_key = conf;
    other_key.sk = "other-sk";
    auto separate = factory.get_or_create(ObjStorageConfigResolver::from_client_conf(other_key));
    ASSERT_TRUE(separate.has_value()) << separate.error();
    EXPECT_NE(separate.value(), first.value());
    EXPECT_EQ(_s3_builds, 2);
}

TEST_F(ObjClientFactoryTest, AzureProviderCachesByContainer) {
    auto& factory = ObjClientFactory::instance();
    auto conf = make_azure_conf("azure-provider-cache.example.com");

    auto first = factory.get_or_create(ObjStorageConfigResolver::from_client_conf(conf));
#ifdef USE_AZURE
    ASSERT_TRUE(first.has_value()) << first.error();
    auto cached = factory.get_or_create(ObjStorageConfigResolver::from_client_conf(conf));
    ASSERT_TRUE(cached.has_value()) << cached.error();
    EXPECT_EQ(cached.value(), first.value());

    auto other_container = conf;
    other_container.bucket = "other-container";
    auto separate =
            factory.get_or_create(ObjStorageConfigResolver::from_client_conf(other_container));
    ASSERT_TRUE(separate.has_value()) << separate.error();
    EXPECT_NE(separate.value(), first.value());
#else
    ASSERT_FALSE(first.has_value());
    EXPECT_TRUE(first.error().is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
#endif
    // Azure confs never reach the S3 provider.
    EXPECT_EQ(_s3_builds, 0);
}

TEST_F(ObjClientFactoryTest, HolderPublishesConfAndClientTogether) {
    auto conf = make_s3_conf("holder-state.example.com");
    ObjClientHolder holder(conf);
    EXPECT_EQ(holder.get(), nullptr);
    EXPECT_EQ(holder.state()->generation, 0U);

    ASSERT_TRUE(holder.init().ok());
    auto initial = holder.state();
    EXPECT_EQ(initial->generation, 1U);
    EXPECT_EQ(initial->conf, conf);
    ASSERT_NE(initial->client, nullptr);

    auto rotated = conf;
    rotated.sk = "rotated-sk";
    ASSERT_TRUE(holder.reset(rotated).ok());
    auto current = holder.state();
    EXPECT_EQ(current->generation, 2U);
    EXPECT_EQ(current->conf.sk, "rotated-sk");
    EXPECT_NE(current->client, initial->client);
    EXPECT_EQ(holder.get(), current->client);
    // A request that took the old snapshot keeps its client and conf.
    EXPECT_EQ(initial->conf.sk, "sk");
    EXPECT_NE(initial->client, nullptr);

    // An unchanged conf publishes nothing.
    ASSERT_TRUE(holder.reset(rotated).ok());
    EXPECT_EQ(holder.state(), current);
}

TEST_F(ObjClientFactoryTest, HolderRejectsResetMergedOntoStaleState) {
    auto conf = make_s3_conf("holder-conflict.example.com");
    ObjClientHolder holder(conf);
    ASSERT_TRUE(holder.init().ok());

    auto outer = conf;
    outer.sk = "outer-sk";
    auto inner = conf;
    inner.sk = "inner-sk";
    // While the outer reset builds its client outside the lock, another reset publishes
    // generation 2. The outer reset was merged onto generation 1, so publishing it would
    // overwrite the inner update with a conf that never saw it.
    Status inner_status;
    _on_s3_build = [&] { inner_status = holder.reset(inner); };

    auto outer_status = holder.reset(outer);

    ASSERT_TRUE(inner_status.ok()) << inner_status;
    EXPECT_TRUE(outer_status.is<ErrorCode::ABORTED>()) << outer_status;
    auto current = holder.state();
    EXPECT_EQ(current->generation, 2U);
    EXPECT_EQ(current->conf.sk, "inner-sk");
}

} // namespace doris::io
