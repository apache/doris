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

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <util/string_util.h>

#include "common/logging.h"

namespace doris {

const static std::string S3_AK = "AWS_ACCESS_KEY";
const static std::string S3_SK = "AWS_SECRET_KEY";
const static std::string S3_ENDPOINT = "AWS_ENDPOINT";
const static std::string S3_REGION = "AWS_REGION";

std::unique_ptr<Aws::S3::S3Client> create_client(const std::map<std::string, std::string>& prop) {
    StringCaseMap<std::string> properties(prop.begin(), prop.end());
    Aws::Auth::AWSCredentials aws_cred;
    Aws::Client::ClientConfiguration aws_config;
    std::unique_ptr<Aws::S3::S3Client> client;
    if (properties.find(S3_AK) != properties.end() && properties.find(S3_SK) != properties.end() &&
        properties.find(S3_ENDPOINT) != properties.end() &&
        properties.find(S3_REGION) != properties.end()) {
        aws_cred.SetAWSAccessKeyId(properties.find(S3_AK)->second);
        aws_cred.SetAWSSecretKey(properties.find(S3_SK)->second);
        DCHECK(!aws_cred.IsExpiredOrEmpty());
        aws_config.endpointOverride = properties.find(S3_ENDPOINT)->second;
        aws_config.region = properties.find(S3_REGION)->second;
        client.reset(new Aws::S3::S3Client(aws_cred, aws_config));
    } else {
        client.reset(nullptr);
    }
    return client;
}
} // end namespace doris
