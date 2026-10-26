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

#include <variant>

#include "util/s3_util.h"

namespace doris::io {
namespace {

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
// ("dummy_region" from FE).
Status validate_obj_client_backend(const AzureClientConfig& azure) {
    if (azure.endpoint.empty()) {
        return Status::InvalidArgument<false>("Invalid azure conf, empty endpoint");
    }
    const auto& shared_key = std::get<AzureSharedKeyConfig>(azure.auth);
    // Either both are provided or both are empty, as for the flat ak/sk.
    if (!azure.account.empty() && shared_key.account_key.empty()) {
        return Status::InvalidArgument<false>("Invalid azure conf, empty account key");
    }
    if (!shared_key.account_key.empty() && azure.account.empty()) {
        return Status::InvalidArgument<false>("Invalid azure conf, empty account name");
    }
    return Status::OK();
}

} // namespace

ObjClientFactory::ObjClientFactory() {
    // Complete AWS SDK initialization before this singleton finishes construction. Function
    // local statics are destroyed in reverse order of construction, so the cached AWS
    // clients held by the providers are released before S3ClientFactory shuts the SDK down.
    S3ClientFactory::instance();
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

} // namespace doris::io
