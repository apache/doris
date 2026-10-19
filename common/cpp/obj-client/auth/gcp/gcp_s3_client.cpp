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

#include "cpp/obj-client/auth/gcp/gcp_s3_client.h"

#include <aws/core/auth/AWSCredentialsProvider.h>

#include <utility>

namespace doris {

GcpS3Client::GcpS3Client(const GcpCredentialConfig& credential, std::string ca_cert_path,
                         Aws::Client::ClientConfiguration aws_config,
                         Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy payload_signing_policy,
                         bool use_virtual_addressing)
        : Aws::S3::S3Client(std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(),
                            std::move(aws_config), payload_signing_policy, use_virtual_addressing),
          _token_provider(credential, ca_cert_path) {}

void GcpS3Client::BuildHttpRequest(
        const Aws::AmazonWebServiceRequest& request,
        const std::shared_ptr<Aws::Http::HttpRequest>& http_request) const {
    Aws::S3::S3Client::BuildHttpRequest(request, http_request);
    auto token = fetch_token();
    if (!token.has_value()) {
        // GCS will return a useful 401 response. The token provider has already
        // logged the underlying credential or metadata error.
        return;
    }
    http_request->SetHeaderValue("Authorization", Aws::String("Bearer ") + token->c_str());
}

std::optional<std::string> GcpS3Client::fetch_token() const {
    return _token_provider.get_token();
}

} // namespace doris
