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

const static std::string S3_AK = "AWS_ACCESS_KEY";
const static std::string S3_SK = "AWS_SECRET_KEY";
const static std::string S3_ENDPOINT = "AWS_ENDPOINT";
const static std::string S3_REGION = "AWS_REGION";

ClientFactory::ClientFactory() {
    _aws_options = Aws::SDKOptions{};
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
    return std::make_shared<Aws::S3::S3Client>(std::move(aws_cred), std::move(aws_config));
}

} // end namespace doris
