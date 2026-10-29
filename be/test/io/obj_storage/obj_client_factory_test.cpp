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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <variant>

#include "cloud/config.h"
#include "common/config.h"
#include "cpp/obj-client/s3_obj_storage_client.h"
#include "cpp/sync_point.h"
#include "io/fs/obj_storage_file_system.h"
#include "io/obj_storage/obj_client_holder.h"
#include "io/obj_storage/obj_storage_config.h"
#include "io/obj_storage/obj_storage_config_resolver.h"
#include "io/obj_storage/obj_storage_uri.h"
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
    return ObjStorageConfigResolver::from_storage_param(param).client;
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

    auto storage = ObjStorageConfigResolver::from_storage_param(param);
    EXPECT_EQ(storage.bucket, "bucket");
    EXPECT_EQ(storage.prefix, "root");
    const auto& config = storage.client;

    ASSERT_TRUE(std::holds_alternative<S3ClientConfig>(config.backend));
    const auto& s3 = std::get<S3ClientConfig>(config.backend);
    EXPECT_EQ(s3.vendor, ObjStorageProvider::OSS);
    EXPECT_EQ(s3.endpoint, "resolve-s3.example.com");
    EXPECT_EQ(s3.region, "us-east-1");
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
    auto config = resolve(make_azure_param("resolve-azure.example.com"));

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
    auto azure_param = make_azure_param("identity-azure.example.com");
    auto azure_with_s3_fields = azure_param;
    azure_with_s3_fields.__set_region("other-region");
    azure_with_s3_fields.__set_token("token");
    azure_with_s3_fields.__set_role_arn("role");
    EXPECT_EQ(resolve(azure_param), resolve(azure_with_s3_fields));

    // The Azure client is bound to one container; an S3 client is not bound to a bucket.
    auto azure_other_container = azure_param;
    azure_other_container.__set_bucket("other-container");
    EXPECT_NE(resolve(azure_param), resolve(azure_other_container));

    auto s3_param = make_s3_param("identity-s3.example.com");
    auto s3_other_bucket = s3_param;
    s3_other_bucket.__set_bucket("other-bucket");
    EXPECT_EQ(resolve(s3_param), resolve(s3_other_bucket));

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

    auto azure_text = resolve(make_azure_param("mask-azure.example.com")).to_string();
    EXPECT_EQ(azure_text.find("YWNjb3VudC1rZXk="), std::string::npos);
    EXPECT_NE(azure_text.find("shared_key"), std::string::npos);
}

TEST_F(ObjClientFactoryTest, S3ProviderCachesByFullConfig) {
    auto& factory = ObjClientFactory::instance();
    auto param = make_s3_param("s3-provider-cache.example.com");

    auto first = factory.get_or_create(resolve(param));
    ASSERT_TRUE(first.has_value()) << first.error();
    EXPECT_EQ(_s3_builds, 1);

    auto other_bucket = param;
    other_bucket.__set_bucket("other-bucket");
    auto shared = factory.get_or_create(resolve(other_bucket));
    ASSERT_TRUE(shared.has_value()) << shared.error();
    EXPECT_EQ(shared.value(), first.value());
    EXPECT_EQ(_s3_builds, 1);

    auto other_key = param;
    other_key.__set_sk("other-sk");
    auto separate = factory.get_or_create(resolve(other_key));
    ASSERT_TRUE(separate.has_value()) << separate.error();
    EXPECT_NE(separate.value(), first.value());
    EXPECT_EQ(_s3_builds, 2);
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

TEST_F(ObjClientFactoryTest, HolderRejectsResetMergedOntoStaleState) {
    auto param = make_s3_param("holder-conflict.example.com");
    ObjClientHolder holder(resolve(param));
    ASSERT_TRUE(holder.init().ok());

    auto outer = param;
    outer.__set_sk("outer-sk");
    auto inner = param;
    inner.__set_sk("inner-sk");
    // While the outer reset builds its client outside the lock, another reset publishes
    // generation 2. The outer reset was merged onto generation 1, so publishing it would
    // overwrite the inner update with a config that never saw it.
    Status inner_status;
    _on_s3_build = [&] { inner_status = holder.reset(resolve(inner)); };

    auto outer_status = holder.reset(resolve(outer));

    ASSERT_TRUE(inner_status.ok()) << inner_status;
    EXPECT_TRUE(outer_status.is<ErrorCode::ABORTED>()) << outer_status;
    auto current = holder.state();
    EXPECT_EQ(current->generation, 2U);
    EXPECT_EQ(secret_key_of(current->config), "inner-sk");
}

TEST_F(ObjClientFactoryTest, ResetMergeKeepsTheStorageTarget) {
    auto current = resolve(make_s3_param("merge-target.example.com"));
    auto update_param = make_s3_param("other-endpoint.example.com");
    update_param.__set_region("other-region");
    update_param.__set_sk("new-sk");
    update_param.__set_use_path_style(true);
    update_param.__set_max_conn(7);

    auto merged = merge_obj_client_config(current, resolve(update_param));
    ASSERT_TRUE(merged.has_value()) << merged.error();
    const auto& s3 = std::get<S3ClientConfig>(merged->backend);
    // The target stays; credentials, addressing and connection options follow the update.
    EXPECT_EQ(s3.endpoint, "merge-target.example.com");
    EXPECT_EQ(s3.region, "us-east-1");
    EXPECT_EQ(s3.auth.secret_key, "new-sk");
    EXPECT_FALSE(s3.use_virtual_addressing);
    EXPECT_EQ(merged->connection.max_connections, 7);

    // Another backend is another storage target, which needs a new binding.
    auto as_azure =
            merge_obj_client_config(current, resolve(make_azure_param("merge-target.example.com")));
    ASSERT_FALSE(as_azure.has_value());
    EXPECT_TRUE(as_azure.error().is<ErrorCode::INVALID_ARGUMENT>()) << as_azure.error();
}

TEST_F(ObjClientFactoryTest, FactoryValidatesBeforeBuilding) {
    auto& factory = ObjClientFactory::instance();
    auto no_region = make_s3_param("validate-s3.example.com");
    no_region.__set_region("");
    EXPECT_FALSE(factory.get_or_create(resolve(no_region)).has_value());
    auto ak_without_sk = make_s3_param("validate-s3.example.com");
    ak_without_sk.__set_sk("");
    EXPECT_FALSE(factory.get_or_create(resolve(ak_without_sk)).has_value());
    // An assumed role needs no static keys.
    auto role = ak_without_sk;
    role.__set_role_arn("role");
    EXPECT_TRUE(factory.get_or_create(resolve(role)).has_value());
    EXPECT_EQ(_s3_builds, 1);

    // Azure has no region; its SharedKey account and key come together, like ak/sk.
    auto azure_no_key = make_azure_param("validate-azure.example.com");
    azure_no_key.__set_sk("");
    auto result = factory.get_or_create(resolve(azure_no_key));
    ASSERT_FALSE(result.has_value());
    EXPECT_TRUE(result.error().is<ErrorCode::INVALID_ARGUMENT>()) << result.error();
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

    // A region is still required for every provider, Azure included.
    props.erase("AWS_REGION");
    EXPECT_FALSE(ObjStorageConfigResolver::from_properties(props, location.value()).has_value());

    cloud::ObjectStoreInfoPB vault;
    vault.set_endpoint("vault.example.com");
    vault.set_region("r");
    vault.set_ak("ak");
    vault.set_sk("sk");
    vault.set_bucket("vault-bucket");
    vault.set_prefix("vault-prefix");
    vault.set_provider(cloud::ObjectStoreInfoPB_Provider_COS);
    auto from_vault = ObjStorageConfigResolver::from_object_store_info(vault);
    EXPECT_EQ(from_vault.bucket, "vault-bucket");
    EXPECT_EQ(from_vault.prefix, "vault-prefix");
    // A storage vault is a Doris internal bucket.
    EXPECT_EQ(from_vault.client.usage, ClientUsage::INTERNAL_STORAGE);
    EXPECT_EQ(from_vault.client.provider(), ObjStorageProvider::COS);
}

} // namespace doris::io
