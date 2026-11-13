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

#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "cloud/config.h"
#include "common/config.h"
#include "cpp/obj-client/auth/azure/azure_auth.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "io/fs/file_writer.h"
#include "io/fs/obj_storage_file_system.h"
#include "io/obj_storage/obj_client_holder.h"
#include "io/obj_storage/obj_storage_config.h"
#include "io/obj_storage/obj_storage_config_resolver.h"
#include "io/obj_storage/obj_storage_uri.h"
#include "util/defer_op.h"
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

// A cold-storage resource as FE sends it; ObjStorageConfigResolver::from_storage_param is one
// of the flat sources that share the mapping into the typed backends.
TS3StorageParam make_s3_param(std::string endpoint) {
    TS3StorageParam param;
    param.__set_endpoint(std::move(endpoint));
    param.__set_region("us-east-1");
    param.__set_ak("ak");
    param.__set_sk("sk");
    param.__set_bucket("bucket");
    param.__set_provider(TObjStorageType::AWS);
    return param;
}

TS3StorageParam make_azure_param(std::string endpoint) {
    auto param = make_s3_param(std::move(endpoint));
    param.__set_provider(TObjStorageType::AZURE);
    param.__set_ak("account");
    param.__set_sk("YWNjb3VudC1rZXk=");
    param.__set_bucket("container");
    return param;
}

ObjClientConfig resolve(const TS3StorageParam& param) {
    auto storage = ObjStorageConfigResolver::from_storage_param(param);
    EXPECT_TRUE(storage.has_value()) << storage.error();
    return std::move(storage).value().client;
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

    // The provider cache is process-wide and outlives a test, and nothing clears it. A test that
    // counts builds or hooks one takes an endpoint nothing has built yet, so that it still sees a
    // cache miss when repeated in one process (GTEST_REPEAT).
    static std::string unique_endpoint(std::string_view name) {
        static std::atomic<int> next {0};
        return fmt::format("{}-{}.example.com", name, next++);
    }

    ObjClientSyncPointGuard _sync_point_guard;
    SyncPoint* _sync_point = SyncPoint::get_instance();
    SyncPoint::CallbackGuard _create_client_callback;
    int _s3_builds = 0;
    std::function<void()> _on_s3_build;
};

TEST_F(ObjClientFactoryTest, ResolvesS3ParamIntoS3Backend) {
    auto param = make_s3_param("resolve-s3.example.com");
    param.__set_provider(TObjStorageType::OSS);
    param.__set_token("token");
    param.__set_role_arn("role");
    param.__set_external_id("external");
    param.__set_cred_provider_type(TCredProviderType::INSTANCE_PROFILE);
    param.__set_use_path_style(true);
    param.__set_max_conn(10);
    param.__set_request_timeout_ms(20);
    param.__set_conn_timeout_ms(30);
    param.__set_root_path("root");

    auto storage = TEST_TRY(ObjStorageConfigResolver::from_storage_param(param));
    EXPECT_EQ(storage.bucket, "bucket");
    EXPECT_EQ(storage.prefix, "root");
    const auto& config = storage.client;

    ASSERT_TRUE(std::holds_alternative<S3ClientConfig>(config.backend));
    const auto& s3 = std::get<S3ClientConfig>(config.backend);
    EXPECT_EQ(s3.vendor, ObjStorageProvider::OSS);
    EXPECT_EQ(s3.endpoint, "resolve-s3.example.com");
    EXPECT_EQ(s3.region, "us-east-1");
    EXPECT_EQ(s3.cache_bucket, param.bucket);
    EXPECT_TRUE(s3.need_override_endpoint);
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
    // A cold-storage resource is an external bucket; storage vaults are internal.
    EXPECT_EQ(config.usage, ClientUsage::EXTERNAL_ACCESS);
    EXPECT_EQ(config.provider(), ObjStorageProvider::OSS);
    EXPECT_EQ(config.endpoint(), "resolve-s3.example.com");
}

TEST_F(ObjClientFactoryTest, ResolvesAzureParamIntoAzureBackend) {
    auto param = make_azure_param("resolve-azure.example.com");
    auto config = resolve(param);

    ASSERT_TRUE(std::holds_alternative<AzureClientConfig>(config.backend));
    const auto& azure = std::get<AzureClientConfig>(config.backend);
    EXPECT_EQ(azure.endpoint, "resolve-azure.example.com");
    EXPECT_EQ(azure.account, "account");
    EXPECT_EQ(azure.container, "container");
    ASSERT_TRUE(std::holds_alternative<AzureSharedKeyConfig>(azure.auth));
    EXPECT_EQ(std::get<AzureSharedKeyConfig>(azure.auth).account_key, param.sk);
    EXPECT_EQ(config.usage, ClientUsage::EXTERNAL_ACCESS);
    EXPECT_EQ(config.provider(), ObjStorageProvider::AZURE);
}

TEST_F(ObjClientFactoryTest, AzureAuthRulesLiveInCommon) {
    // The config layer checks, hashes and names an AzureAuthConfig with these common functions.
    const AzureAuthConfig with_key = AzureSharedKeyConfig {.account_key = "key-a"};
    const AzureAuthConfig without_key = AzureSharedKeyConfig {.account_key = ""};
    EXPECT_FALSE(validate_azure_auth("account", with_key).has_value());
    EXPECT_FALSE(validate_azure_auth("", without_key).has_value());
    auto no_key = validate_azure_auth("account", without_key);
    ASSERT_TRUE(no_key.has_value());
    EXPECT_NE(no_key->find("empty account key"), std::string::npos) << *no_key;
    auto no_account = validate_azure_auth("", with_key);
    ASSERT_TRUE(no_account.has_value());
    EXPECT_NE(no_account->find("empty account name"), std::string::npos) << *no_account;

    auto hash_of = [](std::string_view account, const AzureAuthConfig& auth) {
        uint64_t hash = 0;
        hash_combine_azure_auth(&hash, account, auth);
        return hash;
    };
    EXPECT_EQ(hash_of("account", with_key), hash_of("account", with_key));
    EXPECT_NE(hash_of("account", with_key),
              hash_of("account", AzureSharedKeyConfig {.account_key = "key-b"}));
    EXPECT_NE(hash_of("account", with_key), hash_of("other-account", with_key));

    EXPECT_EQ(azure_auth_type_name(with_key), "shared_key");
    EXPECT_EQ(azure_auth_account_key(with_key), "key-a");
}

TEST_F(ObjClientFactoryTest, RejectsNativeCredentialOnAzureFromMetadataSources) {
    auto& factory = ObjClientFactory::instance();
    auto expect_mismatch = [&](const ObjClientConfig& config) {
        auto result = factory.get_or_create(config);
        ASSERT_FALSE(result.has_value());
        EXPECT_TRUE(result.error().is<ErrorCode::INVALID_ARGUMENT>()) << result.error();
        EXPECT_NE(result.error().to_string().find("provider=GCP"), std::string::npos);
    };
    TGcpCredential gcp_credential;
    gcp_credential.__set_credential_provider_type(TGcpCredentialProviderType::DEFAULT);
    TCredential credential;
    credential.__set_gcp_credential(gcp_credential);
    auto param = make_azure_param("credential-azure.example.com");
    param.__set_credential(credential);
    expect_mismatch(resolve(param));

    cloud::ObjectStoreInfoPB vault;
    vault.set_endpoint("credential-vault.example.com");
    vault.set_region("r");
    vault.set_ak("account");
    vault.set_sk("vault-sk");
    vault.set_bucket("container");
    vault.set_provider(cloud::ObjectStoreInfoPB_Provider_AZURE);
    vault.mutable_credential()->mutable_gcp_credential()->set_credential_provider_type(
            cloud::GcpCredentialPB_CredentialProviderType_DEFAULT);
    auto from_vault = TEST_TRY(ObjStorageConfigResolver::from_object_store_info(vault));
    expect_mismatch(from_vault.client);

    // Creation checks the target first. A refresh first inherits the previous valid target.
    vault.clear_endpoint();
    auto incomplete = TEST_TRY(ObjStorageConfigResolver::from_object_store_info(vault));
    auto create = factory.get_or_create(incomplete.client);
    ASSERT_FALSE(create.has_value());
    EXPECT_EQ(create.error().msg(), "Invalid s3 conf, empty endpoint");
    ObjClientHolder holder(resolve(make_s3_param("valid-target.example.com")));
    ASSERT_TRUE(holder.init().ok());
    auto original = holder.state();
    auto refresh = holder.reset(incomplete.client);
    EXPECT_TRUE(refresh.is<ErrorCode::INVALID_ARGUMENT>()) << refresh;
    EXPECT_NE(refresh.to_string().find("provider=GCP"), std::string::npos);
    EXPECT_EQ(holder.state(), original);
}

TEST_F(ObjClientFactoryTest, ConvertsGcpCredentialFromMetadataSources) {
    // A native GCP credential reaches S3AuthConfig::credential from a resource and a vault, as
    // it does from FE properties (S3ClientFactoryTest covers those).
    const GcpCredentialConfig expected {
            .provider_type = GcpCredentialProviderType::ComputeEngine,
            .impersonation_service_account = "reader@project.iam.gserviceaccount.com",
    };

    TGcpCredential gcp_credential;
    gcp_credential.__set_credential_provider_type(TGcpCredentialProviderType::COMPUTE_ENGINE);
    gcp_credential.__set_impersonation_service_account(expected.impersonation_service_account);
    TCredential credential;
    credential.__set_gcp_credential(gcp_credential);
    auto param = make_s3_param("storage.googleapis.com");
    param.__set_provider(TObjStorageType::GCP);
    param.__set_credential(credential);
    auto from_param = TEST_TRY(ObjStorageConfigResolver::from_storage_param(param));
    const auto& param_s3 = std::get<S3ClientConfig>(from_param.client.backend);
    EXPECT_EQ(param_s3.vendor, ObjStorageProvider::GCP);
    ASSERT_TRUE(std::holds_alternative<GcpCredentialConfig>(param_s3.auth.credential));
    EXPECT_EQ(std::get<GcpCredentialConfig>(param_s3.auth.credential), expected);

    cloud::ObjectStoreInfoPB vault;
    vault.set_endpoint("storage.googleapis.com");
    vault.set_region("us");
    vault.set_bucket("gcp-vault-bucket");
    vault.set_provider(cloud::ObjectStoreInfoPB_Provider_GCP);
    auto* vault_gcp = vault.mutable_credential()->mutable_gcp_credential();
    vault_gcp->set_credential_provider_type(
            cloud::GcpCredentialPB_CredentialProviderType_COMPUTE_ENGINE);
    vault_gcp->set_impersonation_service_account(expected.impersonation_service_account);
    auto from_vault = TEST_TRY(ObjStorageConfigResolver::from_object_store_info(vault));
    const auto& vault_s3 = std::get<S3ClientConfig>(from_vault.client.backend);
    EXPECT_EQ(vault_s3.vendor, ObjStorageProvider::GCP);
    ASSERT_TRUE(std::holds_alternative<GcpCredentialConfig>(vault_s3.auth.credential));
    EXPECT_EQ(std::get<GcpCredentialConfig>(vault_s3.auth.credential), expected);
}

TEST_F(ObjClientFactoryTest, LegacyClientIdentityIncludesAllFlatFields) {
    auto azure_param = make_azure_param("identity-azure.example.com");
    auto changed = azure_param;
    changed.__set_region("other-region");
    EXPECT_NE(resolve(azure_param), resolve(changed));
    changed = azure_param;
    changed.__set_token("token");
    EXPECT_NE(resolve(azure_param), resolve(changed));
    changed = azure_param;
    changed.__set_role_arn("role");
    EXPECT_NE(resolve(azure_param), resolve(changed));

    // The Azure SDK binds a client to a container. The BE S3 cache distinguishes buckets too.
    auto azure_other_container = azure_param;
    azure_other_container.__set_bucket("other-container");
    EXPECT_NE(resolve(azure_param), resolve(azure_other_container));

    auto s3_param = make_s3_param("identity-s3.example.com");
    auto s3_other_bucket = s3_param;
    s3_other_bucket.__set_bucket("other-bucket");
    EXPECT_NE(resolve(s3_param), resolve(s3_other_bucket));

    auto s3_other_region = s3_param;
    s3_other_region.__set_region("other-region");
    EXPECT_NE(resolve(s3_param), resolve(s3_other_region));

    // The same endpoint and credentials on another backend or usage is another client.
    auto s3_as_azure = s3_param;
    s3_as_azure.__set_provider(TObjStorageType::AZURE);
    EXPECT_NE(resolve(s3_param), resolve(s3_as_azure));
    auto s3_internal = resolve(s3_param);
    s3_internal.usage = ClientUsage::INTERNAL_STORAGE;
    EXPECT_NE(resolve(s3_param), s3_internal);
}

TEST_F(ObjClientFactoryTest, ToStringMasksCredentials) {
    auto s3_param = make_s3_param("mask-s3.example.com");
    s3_param.__set_ak("AKIDEXAMPLE");
    s3_param.__set_sk("s3-secret-key");
    s3_param.__set_token("s3-session-token");
    auto s3_text = resolve(s3_param).to_string();
    EXPECT_EQ(s3_text.find("AKIDEXAMPLE"), std::string::npos);
    EXPECT_EQ(s3_text.find("s3-secret-key"), std::string::npos);
    EXPECT_EQ(s3_text.find("s3-session-token"), std::string::npos);

    auto azure_param = make_azure_param("mask-azure.example.com");
    auto azure_text = resolve(azure_param).to_string();
    EXPECT_EQ(azure_text.find(azure_param.sk), std::string::npos);
    EXPECT_NE(azure_text.find("shared_key"), std::string::npos);
}

TEST_F(ObjClientFactoryTest, S3ProviderCachesByFullConfig) {
    auto& factory = ObjClientFactory::instance();
    auto param = make_s3_param(unique_endpoint("s3-provider-cache"));

    auto first = factory.get_or_create(resolve(param));
    ASSERT_TRUE(first.has_value()) << first.error();
    EXPECT_EQ(_s3_builds, 1);

    auto same_bucket = param;
    same_bucket.__set_root_path("other-prefix");
    auto shared = factory.get_or_create(resolve(same_bucket));
    ASSERT_TRUE(shared.has_value()) << shared.error();
    EXPECT_EQ(shared.value(), first.value());
    EXPECT_EQ(_s3_builds, 1);

    auto other_bucket = param;
    other_bucket.__set_bucket("other-bucket");
    auto bucket_client = factory.get_or_create(resolve(other_bucket));
    ASSERT_TRUE(bucket_client.has_value()) << bucket_client.error();
    EXPECT_NE(bucket_client.value(), first.value());
    EXPECT_EQ(_s3_builds, 2);

    auto other_key = param;
    other_key.__set_sk("other-sk");
    auto separate = factory.get_or_create(resolve(other_key));
    ASSERT_TRUE(separate.has_value()) << separate.error();
    EXPECT_NE(separate.value(), first.value());
    EXPECT_EQ(_s3_builds, 3);
}

TEST_F(ObjClientFactoryTest, AzureProviderCachesByContainer) {
    auto& factory = ObjClientFactory::instance();
    auto param = make_azure_param("azure-provider-cache.example.com");

    auto first = factory.get_or_create(resolve(param));
#ifdef USE_AZURE
    ASSERT_TRUE(first.has_value()) << first.error();
    auto cached = factory.get_or_create(resolve(param));
    ASSERT_TRUE(cached.has_value()) << cached.error();
    EXPECT_EQ(cached.value(), first.value());

    auto other_region = param;
    other_region.__set_region("west");
    auto by_region = TEST_TRY(factory.get_or_create(resolve(other_region)));
    EXPECT_NE(by_region, first.value());

    auto other_container = param;
    other_container.__set_bucket("other-container");
    auto separate = factory.get_or_create(resolve(other_container));
    ASSERT_TRUE(separate.has_value()) << separate.error();
    EXPECT_NE(separate.value(), first.value());
#else
    ASSERT_FALSE(first.has_value());
    EXPECT_TRUE(first.error().is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
#endif
    // Azure confs never reach the S3 provider.
    EXPECT_EQ(_s3_builds, 0);
}

TEST_F(ObjClientFactoryTest, AzureProviderSeparatesRotatedSasTokens) {
#ifdef USE_AZURE
    ObjClientConfig config {
            .connection = {},
            .usage = ClientUsage::EXTERNAL_ACCESS,
            .backend =
                    AzureClientConfig {
                            .endpoint = "https://acct.blob.core.windows.net",
                            .account = "acct",
                            .container = "container",
                            .auth = AzureSasConfig {.token = "se=2099-12-31&sig=first",
                                                    .expires_at_ms = std::nullopt},
                    },
    };
    auto& factory = ObjClientFactory::instance();
    auto first = TEST_TRY(factory.get_or_create(config));
    EXPECT_EQ(TEST_TRY(factory.get_or_create(config)), first);
    auto& sas = std::get<AzureSasConfig>(std::get<AzureClientConfig>(config.backend).auth);
    sas.token = "se=2099-12-31&sig=second";
    EXPECT_NE(TEST_TRY(factory.get_or_create(config)), first);
#else
    GTEST_SKIP() << "Azure is not built";
#endif
}

TEST_F(ObjClientFactoryTest, AzureClientsFollowTheBeRateLimitPolicy) {
#ifdef USE_AZURE
    const auto deploy_mode = config::deploy_mode;
    const auto cloud_unique_id = config::cloud_unique_id;
    Defer restore([&] {
        config::deploy_mode = deploy_mode;
        config::cloud_unique_id = cloud_unique_id;
    });
    // The policy S3 clients follow (apply_rate_limit_policy): every client outside cloud mode,
    // only internal storage in cloud mode. Each case has its own endpoint because the provider
    // cache is process-wide.
    auto check = [](bool cloud_mode, ClientUsage usage, bool expect_limited, std::string endpoint) {
        config::deploy_mode = cloud_mode ? "cloud" : "";
        config::cloud_unique_id.clear();
        auto client_config = resolve(make_azure_param(std::move(endpoint)));
        client_config.usage = usage;
        auto client = ObjClientFactory::instance().get_or_create(client_config);
        ASSERT_TRUE(client.has_value()) << client.error();
        EXPECT_EQ(std::dynamic_pointer_cast<RateLimitedObjStorageClient>(client.value()) != nullptr,
                  expect_limited);
    };
    check(false, ClientUsage::EXTERNAL_ACCESS, true, "non-cloud-external-azure.example.com");
    check(true, ClientUsage::INTERNAL_STORAGE, true, "cloud-internal-azure.example.com");
    check(true, ClientUsage::EXTERNAL_ACCESS, false, "cloud-external-azure.example.com");
#else
    GTEST_SKIP() << "Azure is not built";
#endif
}

const std::string& secret_key_of(const ObjClientConfig& config) {
    return std::get<S3ClientConfig>(config.backend).auth.secret_key;
}

TEST_F(ObjClientFactoryTest, HolderPublishesConfigAndClientTogether) {
    auto param = make_s3_param("holder-state.example.com");
    auto config = resolve(param);
    ObjClientHolder holder(config);
    EXPECT_EQ(holder.get(), nullptr);
    EXPECT_EQ(holder.state()->generation, 0U);

    ASSERT_TRUE(holder.init().ok());
    auto initial = holder.state();
    EXPECT_EQ(initial->generation, 1U);
    EXPECT_EQ(initial->config, config);
    ASSERT_NE(initial->client, nullptr);

    auto rotated = param;
    rotated.__set_sk("rotated-sk");
    ASSERT_TRUE(holder.reset(resolve(rotated)).ok());
    auto current = holder.state();
    EXPECT_EQ(current->generation, 2U);
    EXPECT_EQ(secret_key_of(current->config), "rotated-sk");
    EXPECT_NE(current->client, initial->client);
    EXPECT_EQ(holder.get(), current->client);
    // A request that took the old snapshot keeps its client and config.
    EXPECT_EQ(secret_key_of(initial->config), "sk");
    EXPECT_NE(initial->client, nullptr);

    // An unchanged config publishes nothing.
    ASSERT_TRUE(holder.reset(resolve(rotated)).ok());
    EXPECT_EQ(holder.state(), current);
}

TEST_F(ObjClientFactoryTest, OverlappingResetsLetTheLastPublishWin) {
    auto param = make_s3_param(unique_endpoint("holder-overlap"));
    ObjClientHolder holder(resolve(param));
    ASSERT_TRUE(holder.init().ok());

    auto outer = param;
    outer.__set_sk("outer-sk");
    auto inner = param;
    inner.__set_sk("inner-sk");
    // While the outer reset builds its client outside the lock, another reset publishes
    // generation 2. The outer reset then publishes generation 3 on top of it: both succeed and
    // the last one stays, with its whole update.
    Status inner_status;
    _on_s3_build = [&] { inner_status = holder.reset(resolve(inner)); };

    auto outer_status = holder.reset(resolve(outer));

    ASSERT_TRUE(inner_status.ok()) << inner_status;
    ASSERT_TRUE(outer_status.ok()) << outer_status;
    auto current = holder.state();
    EXPECT_EQ(current->generation, 3U);
    EXPECT_EQ(secret_key_of(current->config), "outer-sk");
    EXPECT_EQ(holder.get(), current->client);
}

TEST_F(ObjClientFactoryTest, ResetMergeKeepsTheStorageTarget) {
    auto current = resolve(make_s3_param("merge-target.example.com"));
    auto update_param = make_s3_param("other-endpoint.example.com");
    update_param.__set_region("other-region");
    update_param.__set_bucket("other-bucket");
    update_param.__set_sk("new-sk");
    update_param.__set_use_path_style(true);
    update_param.__set_max_conn(7);

    auto merged = merge_obj_client_config(current, resolve(update_param));
    const auto& s3 = std::get<S3ClientConfig>(merged.backend);
    // The target stays; credentials, addressing and connection options follow the update.
    EXPECT_EQ(s3.endpoint, "merge-target.example.com");
    EXPECT_EQ(s3.region, "us-east-1");
    EXPECT_EQ(s3.cache_bucket, update_param.bucket);
    EXPECT_EQ(s3.auth.secret_key, "new-sk");
    EXPECT_FALSE(s3.use_virtual_addressing);
    EXPECT_EQ(merged.connection.max_connections, 7);

    // Azure keeps its endpoint; the account, container and key follow the update, so a key
    // rotation is not merged away into an unchanged config.
    auto current_azure = resolve(make_azure_param("merge-azure.example.com"));
    auto azure_update_param = make_azure_param("other-azure.example.com");
    azure_update_param.__set_ak("other-account");
    azure_update_param.__set_sk("rotated-azure-key");
    azure_update_param.__set_bucket("other-container");
    auto merged_azure = merge_obj_client_config(current_azure, resolve(azure_update_param));
    const auto& azure = std::get<AzureClientConfig>(merged_azure.backend);
    EXPECT_EQ(azure.endpoint, "merge-azure.example.com");
    EXPECT_EQ(azure.account, "other-account");
    EXPECT_EQ(azure.container, "other-container");
    EXPECT_EQ(std::get<AzureSharedKeyConfig>(azure.auth).account_key, azure_update_param.sk);
}

TEST_F(ObjClientFactoryTest, LegacyAzureMetadataValidatesTheEffectiveRegion) {
    auto& factory = ObjClientFactory::instance();
    auto param = make_azure_param(unique_endpoint("legacy-azure-region"));
    param.__set_region("");
    auto resource_update = resolve(param);
    auto missing = factory.get_or_create(resource_update);
    ASSERT_FALSE(missing.has_value());
    EXPECT_EQ(missing.error().msg(), "Invalid s3 conf, empty region");

    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB_Provider_AZURE);
    vault.set_endpoint(param.endpoint);
    vault.set_ak(param.ak);
    vault.set_sk(param.sk);
    vault.set_bucket("rotated-container");
    auto no_region = TEST_TRY(ObjStorageConfigResolver::from_object_store_info(vault));
    auto initial = factory.get_or_create(no_region.client);
    ASSERT_FALSE(initial.has_value());
    EXPECT_EQ(initial.error().msg(), "Invalid s3 conf, empty region");

    param.__set_region("initial-region");
    ObjClientHolder holder(resolve(param));
#ifdef USE_AZURE
    ASSERT_TRUE(holder.init().ok());
    auto before = holder.state();
    ASSERT_TRUE(holder.reset(resource_update).ok());
    EXPECT_EQ(holder.state(), before);
    ASSERT_TRUE(holder.reset(no_region.client).ok());
    auto updated = holder.state();
    const auto& azure = std::get<AzureClientConfig>(updated->config.backend);
    ASSERT_TRUE(azure.legacy.has_value());
    EXPECT_EQ(azure.legacy->region, "initial-region");
    EXPECT_EQ(azure.container, "rotated-container");
    auto to_s3 = make_s3_param("unused-storage-target.example.com");
    ASSERT_TRUE(holder.reset(resolve(to_s3)).ok());
    const auto& s3 = std::get<S3ClientConfig>(holder.state()->config.backend);
    EXPECT_EQ(s3.region, "initial-region");
    EXPECT_EQ(s3.endpoint, param.endpoint);
#else
    EXPECT_TRUE(holder.init().is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
#endif
}

TEST_F(ObjClientFactoryTest, AzurePresigningUsesRequestedEndpoint) {
#ifdef USE_AZURE
    auto param = make_azure_param("presign-internal.aliyuncs.com");
    auto storage = TEST_TRY(ObjStorageConfigResolver::from_storage_param(param));
    auto fs = TEST_TRY(ObjStorageFileSystem::create(std::move(storage), "presign-azure"));

    auto public_url = fs->generate_presigned_url("error_log/id", 60, true);
    EXPECT_TRUE(public_url.starts_with("https://presign.aliyuncs.com/container/error_log/id?"));
    EXPECT_NE(public_url.find("sig="), std::string::npos);
    auto private_url = fs->generate_presigned_url("error_log/id", 60, false);
    EXPECT_TRUE(private_url.starts_with(
            "https://presign-internal.aliyuncs.com/container/error_log/id?"));
#else
    GTEST_SKIP() << "Azure is not built";
#endif
}

TEST_F(ObjClientFactoryTest, CrossBackendRefreshPreservesTheFileSystemAndItsOriginalTarget) {
    auto original = make_s3_param(unique_endpoint("resource-round-trip"));
    original.__set_root_path("root");
    original.__set_region("original-region");
    auto storage = TEST_TRY(ObjStorageConfigResolver::from_storage_param(original));
    std::get<S3ClientConfig>(storage.client.backend).need_override_endpoint = false;
    auto fs = TEST_TRY(ObjStorageFileSystem::create(storage, "resource"));
    auto holder = fs->client_holder();
    auto initial = holder->state();

    // A key pair that fails validation must leave the previous client and config published.
    auto bad = make_azure_param("unbound.example.com");
    bad.__set_sk("");
    auto failure = holder->reset(resolve(bad));
    EXPECT_TRUE(failure.is<ErrorCode::INVALID_ARGUMENT>()) << failure;
    EXPECT_EQ(holder->state(), initial);

    auto azure_update = make_azure_param("new-endpoint.example.com");
    azure_update.__set_bucket("new-container");
    azure_update.__set_root_path("different-root");
    auto status = holder->reset(resolve(azure_update));
#ifdef USE_AZURE
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(fs->client_holder(), holder);
    EXPECT_EQ(fs->bucket(), original.bucket);
    EXPECT_EQ(fs->prefix(), original.root_path);
    auto updated = holder->state();
    EXPECT_NE(updated->client, initial->client);
    const auto& azure = std::get<AzureClientConfig>(updated->config.backend);
    EXPECT_EQ(azure.endpoint, original.endpoint);
    EXPECT_EQ(azure.container, azure_update.bucket);
    EXPECT_EQ(azure.account, azure_update.ak);

    auto back = make_s3_param("third-endpoint.example.com");
    back.__set_region("third-region");
    back.__set_bucket("third-bucket");
    back.__set_sk("rotated");
    ASSERT_TRUE(holder->reset(resolve(back)).ok());
    const auto& s3 = std::get<S3ClientConfig>(holder->state()->config.backend);
    EXPECT_EQ(s3.endpoint, original.endpoint);
    EXPECT_EQ(s3.region, original.region);
    EXPECT_FALSE(s3.need_override_endpoint);
    EXPECT_EQ(s3.cache_bucket, back.bucket);
    EXPECT_EQ(s3.auth.secret_key, back.sk);
    EXPECT_EQ(fs->bucket(), original.bucket);
#else
    EXPECT_TRUE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status;
    EXPECT_EQ(holder->state(), initial);
#endif
}

TEST_F(ObjClientFactoryTest, FactoryValidatesBeforeBuilding) {
    auto& factory = ObjClientFactory::instance();
    const auto endpoint = unique_endpoint("validate-s3");
    auto no_region = make_s3_param(endpoint);
    no_region.__set_region("");
    EXPECT_FALSE(factory.get_or_create(resolve(no_region)).has_value());
    auto ak_without_sk = make_s3_param(endpoint);
    ak_without_sk.__set_sk("");
    EXPECT_FALSE(factory.get_or_create(resolve(ak_without_sk)).has_value());
    // An assumed role needs no static keys.
    auto role = ak_without_sk;
    role.__set_role_arn("role");
    EXPECT_TRUE(factory.get_or_create(resolve(role)).has_value());
    EXPECT_EQ(_s3_builds, 1);

    // Legacy Azure uses the same role exemption as the S3-shaped input.
    auto azure_no_key = make_azure_param("validate-azure.example.com");
    azure_no_key.__set_sk("");
    auto result = factory.get_or_create(resolve(azure_no_key));
    ASSERT_FALSE(result.has_value());
    EXPECT_TRUE(result.error().is<ErrorCode::INVALID_ARGUMENT>()) << result.error();

    azure_no_key.__set_role_arn("legacy-role");
    auto legacy_role = resolve(azure_no_key);
    EXPECT_TRUE(legacy_role.has_role_arn());
    EXPECT_TRUE(validate_obj_client_config(legacy_role).ok());

    // Neither backend builds a client without an endpoint.
    auto s3_no_endpoint = factory.get_or_create(resolve(make_s3_param("")));
    ASSERT_FALSE(s3_no_endpoint.has_value());
    EXPECT_TRUE(s3_no_endpoint.error().is<ErrorCode::INVALID_ARGUMENT>()) << s3_no_endpoint.error();
    EXPECT_NE(s3_no_endpoint.error().to_string().find("empty endpoint"), std::string::npos);
    auto azure_no_endpoint = factory.get_or_create(resolve(make_azure_param("")));
    ASSERT_FALSE(azure_no_endpoint.has_value());
    EXPECT_TRUE(azure_no_endpoint.error().is<ErrorCode::INVALID_ARGUMENT>())
            << azure_no_endpoint.error();
    EXPECT_NE(azure_no_endpoint.error().to_string().find("empty endpoint"), std::string::npos);
    EXPECT_EQ(_s3_builds, 1);
}

TEST_F(ObjClientFactoryTest, PropertiesReportMissingEndpointBeforeRegion) {
    auto location = ObjStorageUri::parse_s3("s3://bucket/key");
    ASSERT_TRUE(location.has_value()) << location.error();
    for (const char* provider : {"AWS", "Azure"}) {
        SCOPED_TRACE(provider);
        std::map<std::string, std::string> props = {{"provider", provider}};
        auto no_endpoint = ObjStorageConfigResolver::from_properties(props, location.value());
        ASSERT_FALSE(no_endpoint.has_value());
        EXPECT_TRUE(no_endpoint.error().is<ErrorCode::INVALID_ARGUMENT>());
        EXPECT_EQ(no_endpoint.error().msg(), "Invalid s3 conf, empty endpoint");

        props["AWS_ENDPOINT"] = "props.example.com";
        auto no_region = ObjStorageConfigResolver::from_properties(props, location.value());
        ASSERT_FALSE(no_region.has_value());
        EXPECT_TRUE(no_region.error().is<ErrorCode::INVALID_ARGUMENT>());
        EXPECT_EQ(no_region.error().msg(), "Invalid s3 conf, empty region");
    }
}

TEST_F(ObjClientFactoryTest, ResolvesPropertiesAndVaults) {
    auto location = ObjStorageUri::parse_s3("s3://props-bucket/dir/file");
    ASSERT_TRUE(location.has_value()) << location.error();
    std::map<std::string, std::string> props = {{"AWS_ENDPOINT", "props.example.com"},
                                                {"AWS_REGION", "r"},
                                                {"AWS_ACCESS_KEY", "ak"},
                                                {"AWS_SECRET_KEY", "sk"},
                                                {"use_path_style", "true"},
                                                {"AWS_MAX_CONNECTIONS", "8"}};
    auto from_props = ObjStorageConfigResolver::from_properties(props, location.value());
    ASSERT_TRUE(from_props.has_value()) << from_props.error();
    EXPECT_EQ(from_props->bucket, "props-bucket");
    EXPECT_EQ(from_props->prefix, "");
    EXPECT_EQ(std::get<S3ClientConfig>(from_props->client.backend).cache_bucket,
              from_props->bucket);
    EXPECT_FALSE(std::get<S3ClientConfig>(from_props->client.backend).use_virtual_addressing);
    EXPECT_EQ(from_props->client.connection.max_connections, 8);

    // provider=azure selects Azure; the bucket of the location is the container.
    props["provider"] = "azure";
    auto azure = ObjStorageConfigResolver::from_properties(props, location.value());
    ASSERT_TRUE(azure.has_value()) << azure.error();
    EXPECT_EQ(std::get<AzureClientConfig>(azure->client.backend).container, "props-bucket");

    // Properties are checked when they are converted with the typed rules of the backend, e.g.
    // an Azure account key needs an account name.
    auto without_account = props;
    without_account.erase("AWS_ACCESS_KEY");
    auto no_account = ObjStorageConfigResolver::from_properties(without_account, location.value());
    ASSERT_FALSE(no_account.has_value());
    EXPECT_TRUE(no_account.error().is<ErrorCode::INVALID_ARGUMENT>()) << no_account.error();

    // Azure properties cannot carry a provider-native credential, which only S3 has a field for.
    auto with_gcp_credential = props;
    with_gcp_credential["gs.credential_provider_type"] = "DEFAULT";
    auto gcp_on_azure =
            ObjStorageConfigResolver::from_properties(with_gcp_credential, location.value());
    ASSERT_FALSE(gcp_on_azure.has_value());
    EXPECT_NE(gcp_on_azure.error().to_string().find("provider=GCP"), std::string::npos)
            << gcp_on_azure.error();

    cloud::ObjectStoreInfoPB vault;
    vault.set_endpoint("vault.example.com");
    vault.set_region("r");
    vault.set_ak("ak");
    vault.set_sk("sk");
    vault.set_bucket("vault-bucket");
    vault.set_prefix("vault-prefix");
    vault.set_provider(cloud::ObjectStoreInfoPB_Provider_COS);
    vault.set_role_arn("vault-role");
    vault.set_external_id("vault-external-id");
    vault.set_cred_provider_type(cloud::CredProviderTypePB::INSTANCE_PROFILE);
    auto from_vault = TEST_TRY(ObjStorageConfigResolver::from_object_store_info(vault));
    EXPECT_EQ(from_vault.bucket, "vault-bucket");
    EXPECT_EQ(from_vault.prefix, "vault-prefix");
    EXPECT_EQ(std::get<S3ClientConfig>(from_vault.client.backend).cache_bucket, from_vault.bucket);
    // A storage vault is a Doris internal bucket.
    EXPECT_EQ(from_vault.client.usage, ClientUsage::INTERNAL_STORAGE);
    EXPECT_EQ(from_vault.client.provider(), ObjStorageProvider::COS);
    const auto& vault_auth = std::get<S3ClientConfig>(from_vault.client.backend).auth;
    EXPECT_EQ(vault_auth.role_arn, "vault-role");
    EXPECT_EQ(vault_auth.external_id, "vault-external-id");
    EXPECT_EQ(vault_auth.provider_type, CredProviderType::InstanceProfile);
}

TEST_F(ObjClientFactoryTest, ExpiredSasFailsBeforeAnyClientIsBuilt) {
    ObjClientConfig config {.connection = {},
                            .usage = ClientUsage::EXTERNAL_ACCESS,
                            .backend = AzureClientConfig {
                                    .endpoint = "https://acct.blob.core.windows.net",
                                    .account = "",
                                    .container = "container",
                                    .auth = AzureSasConfig {.token = "se=1999-12-31&sig=abc",
                                                            .expires_at_ms = std::nullopt},
                            }};
    ObjClientHolder holder(config);
    auto status = holder.init();
    // The credential check runs before the Azure SDK is touched, so the result is the same
    // with or without BUILD_AZURE.
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    EXPECT_EQ(holder.get(), nullptr);
}

TEST_F(ObjClientFactoryTest, VaultSseSettingIsLoggedAndReusesTheClient) {
    cloud::ObjectStoreInfoPB vault;
    vault.set_provider(cloud::ObjectStoreInfoPB_Provider_S3);
    vault.set_endpoint(unique_endpoint("vault-sse"));
    vault.set_region("us-east-1");
    vault.set_bucket("bucket");

    auto storage = TEST_TRY(ObjStorageConfigResolver::from_object_store_info(vault));
    EXPECT_NE(storage.to_string().find("sse_enabled=false"), std::string::npos);
    auto& factory = ObjClientFactory::instance();
    auto client = TEST_TRY(factory.get_or_create(storage.client));

    vault.set_sse_enabled(true);
    auto updated = TEST_TRY(ObjStorageConfigResolver::from_object_store_info(vault));
    EXPECT_NE(updated.to_string().find("sse_enabled=true"), std::string::npos);
    auto cached = TEST_TRY(factory.get_or_create(updated.client));
    EXPECT_EQ(client, cached);
}

} // namespace doris::io
