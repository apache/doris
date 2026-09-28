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
#include <aws/core/http/HttpRequest.h>
#include <aws/s3/S3Client.h>

#include <memory>
#include <optional>
#include <string>

#include "cpp/obj-client/auth/gcp/gcp_auth.h"
#include "cpp/obj-client/auth/gcp/gcp_token_provider.h"

namespace doris {

// GCS accepts OAuth2 bearer tokens on its S3-compatible XML API. The client uses
// anonymous AWS credentials so AWSAuthV4Signer returns without adding SigV4, and
// BuildHttpRequest injects the bearer header immediately before that signing step.
class GcpS3Client : public Aws::S3::S3Client {
public:
    GcpS3Client(const GcpCredentialConfig& credential, std::string ca_cert_path,
                Aws::Client::ClientConfiguration aws_config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy payload_signing_policy,
                bool use_virtual_addressing);

protected:
    void BuildHttpRequest(
            const Aws::AmazonWebServiceRequest& request,
            const std::shared_ptr<Aws::Http::HttpRequest>& http_request) const override;

    // Test seam for asserting the outgoing header without relying on ADC or metadata.
    virtual std::optional<std::string> fetch_token() const;

private:
    GcpTokenProvider _token_provider;
};

} // namespace doris
