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

#include <variant>

#include "cloud/config.h"
#include "common/config.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "util/s3_rate_limiter_manager.h"
#include "util/s3_util.h"

namespace doris::io {
namespace {

ObjStorageResponse make_be_rate_limit_response(S3RateLimitType type,
                                               S3RateLimitRejectReason reason) {
    const auto* limit_type = reason == S3RateLimitRejectReason::QPS ? "QPS" : "bytes";
    // A local admission rejection is not an S3 HTTP 429. Keep the merged #65420 behavior so S3
    // readers do not retry it as provider throttling.
    return ObjStorageResponse::rate_limit(
            ErrorCode::EXCEEDED_LIMIT, 0,
            fmt::format("s3 {} request exceeds {} limit, rejected by BE rate limiter",
                        to_string(type), limit_type));
}

class BeObjStorageRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    ObjStorageAdmission acquire(S3RateLimitType type, size_t estimated_bytes) const override {
        auto guard = std::make_shared<S3RateLimitGuard>(type, estimated_bytes);
        if (!guard->ok()) {
            return ObjStorageAdmission {
                    .resp = make_be_rate_limit_response(type, guard->reject_reason()),
            };
        }
        return ObjStorageAdmission {
                .settle = [guard = std::move(guard)](
                                  size_t actual_bytes) { guard->settle(actual_bytes); },
        };
    }
};

// The rules every client request is checked against before anything is built or looked up;
// they are the checks the former S3ClientFactory::create applied to the flat configuration.
Status validate_obj_client_backend(const S3ClientConfig& s3) {
    if (s3.endpoint.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty endpoint");
    }
    if (s3.region.empty()) {
        return Status::InvalidArgument<false>("Invalid s3 conf, empty region");
    }
    ObjCredentialValidationContext credential_context {
            .provider = to_obj_credential_provider(s3.vendor),
            .ak = s3.auth.access_key,
            .sk = s3.auth.secret_key,
            .token = s3.auth.session_token,
            .has_aws_role_arn = !s3.auth.role_arn.empty(),
            .has_aws_external_id = !s3.auth.external_id.empty(),
            .has_aws_credential_provider = s3.auth.provider_type != CredProviderType::Default,
    };
    if (auto error = validate_obj_credential_config(s3.auth.credential, credential_context);
        error.has_value()) {
        return Status::InvalidArgument<false>(*error);
    }
    if (s3.auth.role_arn.empty()) {
        // Either both credentials are provided or both are empty (anonymous access)
        if (!s3.auth.access_key.empty() && s3.auth.secret_key.empty()) {
            return Status::InvalidArgument<false>("Invalid s3 conf, empty sk");
        }
        if (!s3.auth.secret_key.empty() && s3.auth.access_key.empty()) {
            return Status::InvalidArgument<false>("Invalid s3 conf, empty ak");
        }
    }
    return Status::OK();
}

// Azure has no region, which the flat configuration used to require with a placeholder value
// ("dummy_region" from FE). Typed Azure configurations pass AzureConfigValidator before they get
// here, so these weaker rules only matter for the flat sources.
Status validate_obj_client_backend(const AzureClientConfig& azure) {
    if (azure.endpoint.empty()) {
        return Status::InvalidArgument<false>("Invalid azure conf, empty endpoint");
    }
    if (const auto* shared_key = std::get_if<AzureSharedKeyConfig>(&azure.auth)) {
        // Either both are provided or both are empty, as for the flat ak/sk.
        if (!azure.account.empty() && shared_key->account_key.empty()) {
            return Status::InvalidArgument<false>("Invalid azure conf, empty account key");
        }
        if (!shared_key->account_key.empty() && azure.account.empty()) {
            return Status::InvalidArgument<false>("Invalid azure conf, empty account name");
        }
    }
    return Status::OK();
}

} // namespace

ObjClientFactory::ObjClientFactory() {
    // Complete AWS SDK initialization before this singleton finishes construction. Function
    // local statics are destroyed in reverse order of construction, so the cached AWS
    // clients held by the providers are released before AwsSdkManager shuts the SDK down.
    AwsSdkManager::instance();
}

ObjClientFactory& ObjClientFactory::instance() {
    static ObjClientFactory ret;
    return ret;
}

Result<std::shared_ptr<ObjStorageClient>> ObjClientFactory::get_or_create(
        const ObjClientConfig& config) {
    RETURN_IF_ERROR_RESULT(
            std::visit([](const auto& backend) { return validate_obj_client_backend(backend); },
                       config.backend));
#ifdef BE_TEST
    {
        std::lock_guard lock(_test_lock);
        if (_test_client_creator) {
            return _test_client_creator(config);
        }
    }
#endif
    return std::visit([&](const auto& backend) { return _dispatch(config, backend); },
                      config.backend);
}

#ifdef BE_TEST
void ObjClientFactory::set_client_creator_for_test(
        std::function<std::shared_ptr<ObjStorageClient>(const ObjClientConfig&)> creator) {
    std::lock_guard lock(_test_lock);
    _test_client_creator = std::move(creator);
}
#endif

Result<std::shared_ptr<ObjStorageClient>> ObjClientFactory::_dispatch(const ObjClientConfig& config,
                                                                      const S3ClientConfig& s3) {
    return _s3.get_or_create(config, s3);
}

Result<std::shared_ptr<ObjStorageClient>> ObjClientFactory::_dispatch(
        const ObjClientConfig& config, const AzureClientConfig& azure) {
    return _azure.get_or_create(config, azure);
}

std::shared_ptr<ObjStorageClient> apply_rate_limit_policy(std::shared_ptr<ObjStorageClient> client,
                                                          ClientUsage usage) {
    if (!config::is_cloud_mode() || usage == ClientUsage::INTERNAL_STORAGE) {
        return std::make_shared<RateLimitedObjStorageClient>(
                std::move(client), std::make_shared<BeObjStorageRateLimitPolicy>());
    }
    return client;
}

} // namespace doris::io

namespace doris::config {

// The cache takes the new capacity at once, so a lowered one evicts immediately even if only
// hits follow, and a publish that read the old value from the config cannot exceed it. The
// callback runs after the new value passed its validator, so it is always positive.
DEFINE_ON_UPDATE(azure_client_cache_capacity, [](int32_t /*old_value*/, int32_t new_value) {
    io::ObjClientFactory::instance().set_azure_client_cache_capacity(
            static_cast<size_t>(new_value));
});

} // namespace doris::config
