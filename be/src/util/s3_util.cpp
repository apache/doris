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

#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <util/string_util.h>

#include "common/config.h"
#include "util/logging.h"

namespace doris {

const static std::string USE_PATH_STYLE = "use_path_style";

ClientFactory::ClientFactory() {
    _aws_options = Aws::SDKOptions {};
    Aws::Utils::Logging::LogLevel logLevel =
            static_cast<Aws::Utils::Logging::LogLevel>(config::aws_log_level);
    _aws_options.loggingOptions.logLevel = logLevel;
    _aws_options.loggingOptions.logger_create_fn = [logLevel] {
        return std::make_shared<DorisAWSLogger>(logLevel);
    };
    Aws::InitAPI(_aws_options);
}

ClientFactory::~ClientFactory() {
    Aws::ShutdownAPI(_aws_options);
}

ClientFactory& ClientFactory::instance() {
    static ClientFactory ret;
    return ret;
}

bool ClientFactory::is_s3_conf_valid(const std::map<std::string, std::string>& prop) {
    StringCaseMap<std::string> properties(prop.begin(), prop.end());
    if (properties.find(S3_AK) == properties.end() || properties.find(S3_SK) == properties.end() ||
        properties.find(S3_ENDPOINT) == properties.end() ||
        properties.find(S3_REGION) == properties.end()) {
        DCHECK(false) << "aws properties is incorrect.";
        LOG(ERROR) << "aws properties is incorrect.";
        return false;
    }
    return true;
}

std::shared_ptr<Aws::S3::S3Client> ClientFactory::create(
        const std::map<std::string, std::string>& prop) {
    StringCaseMap<std::string> properties(prop.begin(), prop.end());
    if (properties.find(S3_AK) == properties.end() || properties.find(S3_SK) == properties.end() ||
        properties.find(S3_ENDPOINT) == properties.end() ||
        properties.find(S3_REGION) == properties.end()) {
        DCHECK(false) << "aws properties is incorrect.";
        LOG(ERROR) << "aws properties is incorrect.";
    }
    Aws::Auth::AWSCredentials aws_cred(properties.find(S3_AK)->second,
                                       properties.find(S3_SK)->second);
    DCHECK(!aws_cred.IsExpiredOrEmpty());

    Aws::Client::ClientConfiguration aws_config;
    aws_config.endpointOverride = properties.find(S3_ENDPOINT)->second;
    aws_config.region = properties.find(S3_REGION)->second;
    if (properties.find(S3_MAX_CONN_SIZE) != properties.end()) {
        aws_config.maxConnections = std::atoi(properties.find(S3_MAX_CONN_SIZE)->second.c_str());
    }
    if (properties.find(S3_REQUEST_TIMEOUT_MS) != properties.end()) {
        aws_config.requestTimeoutMs =
                std::atoi(properties.find(S3_REQUEST_TIMEOUT_MS)->second.c_str());
    }
    if (properties.find(S3_CONN_TIMEOUT_MS) != properties.end()) {
        aws_config.connectTimeoutMs =
                std::atoi(properties.find(S3_CONN_TIMEOUT_MS)->second.c_str());
    }

    // See https://sdk.amazonaws.com/cpp/api/LATEST/class_aws_1_1_s3_1_1_s3_client.html
    bool use_virtual_addressing = true;
    if (properties.find(USE_PATH_STYLE) != properties.end()) {
        use_virtual_addressing = properties.find(USE_PATH_STYLE)->second == "true" ? false : true;
    }
    return std::make_shared<Aws::S3::S3Client>(
            std::move(aws_cred), std::move(aws_config),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, use_virtual_addressing);
}

} // end namespace doris
