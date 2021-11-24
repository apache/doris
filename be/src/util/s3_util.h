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

#pragma once

#include <aws/core/Aws.h>

#include <map>
#include <memory>
#include <string>

namespace Aws {
namespace S3 {
class S3Client;
} // namespace S3
} // namespace Aws

namespace doris {

const static std::string S3_AK = "AWS_ACCESS_KEY";
const static std::string S3_SK = "AWS_SECRET_KEY";
const static std::string S3_ENDPOINT = "AWS_ENDPOINT";
const static std::string S3_REGION = "AWS_REGION";
const static std::string S3_MAX_CONN_SIZE = "AWS_MAX_CONN_SIZE";
const static std::string S3_REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
const static std::string S3_CONN_TIMEOUT_MS = "AWS_CONN_TIMEOUT_MS";

class ClientFactory {
public:
    ~ClientFactory();

    static ClientFactory& instance();

    std::shared_ptr<Aws::S3::S3Client> create(const std::map<std::string, std::string>& prop);

    static bool is_s3_conf_valid(const std::map<std::string, std::string>& prop);
private:
    ClientFactory();

    Aws::SDKOptions _aws_options;
};
std::unique_ptr<Aws::S3::S3Client> create_client(const std::map<std::string, std::string>& prop);

} // end namespace doris
