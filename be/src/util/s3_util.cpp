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

#include "util/s3_util.h"

#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/LogSystemInterface.h>

#ifdef USE_AZURE
#include <azure/core/diagnostics/logger.hpp>
#endif
#include <memory>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/aws_logger.h"
#include "cpp/obj-client/auth/aws_credential_factory.h"
#include "io/obj_storage/obj_storage_config.h"
#include "util/string_util.h"

namespace doris {

AwsSdkManager::AwsSdkManager() {
    _aws_options = Aws::SDKOptions {};
    auto logLevel = static_cast<Aws::Utils::Logging::LogLevel>(config::aws_log_level);
    _aws_options.loggingOptions.logLevel = logLevel;
    _aws_options.loggingOptions.logger_create_fn = [logLevel] {
        return std::make_shared<DorisAWSLogger>(logLevel);
    };
    // Object keys must be encoded strictly by RFC 3986 in request URIs. The setting is global
    // to the SDK, and InitAPI sets it from these options, so it is not reset to the SDK default
    // false; every object storage client is built after this factory (ObjClientFactory
    // initializes it first).
    _aws_options.httpOptions.compliantRfc3986Encoding = true;
    Aws::InitAPI(_aws_options);
    _get_ca_cert_file_path();

#ifdef USE_AZURE
    auto azureLogLevel =
            static_cast<Azure::Core::Diagnostics::Logger::Level>(config::azure_log_level);
    Azure::Core::Diagnostics::Logger::SetLevel(azureLogLevel);
    Azure::Core::Diagnostics::Logger::SetListener(
            [&](Azure::Core::Diagnostics::Logger::Level level, const std::string& message) {
                switch (level) {
                case Azure::Core::Diagnostics::Logger::Level::Verbose:
                    LOG(INFO) << message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Informational:
                    LOG(INFO) << message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Warning:
                    LOG(WARNING) << message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Error:
                    LOG(ERROR) << message;
                    break;
                default:
                    LOG(WARNING) << "Unknown level: " << static_cast<int>(level)
                                 << ", message: " << message;
                    break;
                }
            });
#endif
}

AwsSdkManager::~AwsSdkManager() {
    Aws::ShutdownAPI(_aws_options);
}

AwsSdkManager& AwsSdkManager::instance() {
    static AwsSdkManager ret;
    return ret;
}

std::string AwsSdkManager::_get_ca_cert_file_path() {
    std::lock_guard lock(_ca_cert_lock);
    if (_ca_cert_file_path.empty()) {
        _ca_cert_file_path = get_valid_ca_cert_path(doris::split(config::ca_cert_file_paths, ";"));
    }
    return _ca_cert_file_path;
}

AwsCredentialResult AwsSdkManager::create_aws_credentials_provider(const io::S3AuthConfig& auth) {
    auto sts_config = AwsSdkManager::getClientConfiguration();
    auto ca_cert_file_path = _get_ca_cert_file_path();
    if (!ca_cert_file_path.empty()) {
        sts_config.caFile = ca_cert_file_path;
    }
    return AwsCredentialFactory::create({
            .version = config::aws_credentials_provider_version == "v2"
                               ? AwsCredentialProviderVersion::V2
                               : AwsCredentialProviderVersion::V1,
            .access_key = auth.access_key,
            .secret_key = auth.secret_key,
            .session_token = auth.session_token,
            .provider_type = auth.provider_type,
            .role_arn = auth.role_arn,
            .external_id = auth.external_id,
            .empty_credentials = EmptyCredentialsBehavior::ANONYMOUS,
            .sts_client_config = std::move(sts_config),
    });
}

std::string hide_access_key(const std::string& ak) {
    std::string key = ak;
    size_t key_len = key.length();
    size_t reserved_count;
    if (key_len > 7) {
        reserved_count = 6;
    } else if (key_len > 2) {
        reserved_count = key_len - 2;
    } else {
        reserved_count = 0;
    }

    size_t x_count = key_len - reserved_count;
    size_t left_x_count = (x_count + 1) / 2;

    if (left_x_count > 0) {
        key.replace(0, left_x_count, left_x_count, 'x');
    }

    if (x_count - left_x_count > 0) {
        key.replace(key_len - (x_count - left_x_count), x_count - left_x_count,
                    x_count - left_x_count, 'x');
    }
    return key;
}

} // end namespace doris
