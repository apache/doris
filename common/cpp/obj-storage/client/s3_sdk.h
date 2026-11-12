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

#include <aws/core/client/ClientConfiguration.h>

#include <memory>
#include <string>
#include <utility>

#include "cpp/obj-storage/auth/aws_credential_factory.h"

namespace Aws::S3 {
class S3Client;
}

namespace doris {

Aws::Client::ClientConfiguration s3_sdk_config(const ObjStorageConnectionConfig& connection,
                                               const std::string& region, bool retry_slow_down,
                                               bool use_sdk_endpoint = false,
                                               bool sdk_endpoint_http = false);
std::pair<std::shared_ptr<Aws::S3::S3Client>, std::string> make_s3_sdk_client(
        const Aws::Client::ClientConfiguration& sdk, bool use_path_style,
        const AwsCredentialOptions& credential_options);

} // namespace doris
