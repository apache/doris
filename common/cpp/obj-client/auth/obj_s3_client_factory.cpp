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

#include "cpp/obj-client/auth/obj_s3_client_factory.h"

#include <aws/s3/S3Client.h>

#include <utility>

#include "cpp/obj-client/auth/gcp/gcp_s3_client.h"

namespace doris {
namespace {

// Keep the overloads explicit so adding another CredentialConfig alternative
// fails to compile until its client construction path is implemented here.
struct S3ClientBuilder {
    S3ClientBuildContext& context;

    std::shared_ptr<Aws::S3::S3Client> operator()(const std::monostate&) const {
        return std::make_shared<Aws::S3::S3Client>(
                context.fallback_provider, std::move(context.config),
                context.payload_signing_policy, context.use_virtual_addressing);
    }

    std::shared_ptr<Aws::S3::S3Client> operator()(const GcpCredentialConfig& credential) const {
        return std::make_shared<GcpS3Client>(
                credential, context.ca_cert_path, std::move(context.config),
                context.payload_signing_policy, context.use_virtual_addressing);
    }
};

} // namespace

std::shared_ptr<Aws::S3::S3Client> make_s3_client(const CredentialConfig& credential,
                                                  S3ClientBuildContext context) {
    return std::visit(S3ClientBuilder {context}, credential);
}

} // namespace doris
