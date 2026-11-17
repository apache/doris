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

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/client/ClientConfiguration.h>

#include <memory>
#include <string>

#include "cpp/obj-client/auth/obj_credential.h"

namespace Aws::S3 {
class S3Client;
}

namespace doris {

// Neutral inputs shared by BE's S3ClientConf and Cloud's S3Conf. Provider-native
// authentication is selected before construction because AWS credentials providers
// cannot be replaced safely on an already constructed client.
struct S3ClientBuildContext {
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> fallback_provider;
    Aws::Client::ClientConfiguration config;
    Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy payload_signing_policy =
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;
    bool use_virtual_addressing = true;
    std::string ca_cert_path;
};

std::shared_ptr<Aws::S3::S3Client> make_s3_client(const CredentialConfig& credential,
                                                  S3ClientBuildContext context);

} // namespace doris
