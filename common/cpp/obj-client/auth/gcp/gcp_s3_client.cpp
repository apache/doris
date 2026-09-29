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
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationResult.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/GetBucketVersioningResult.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>

#include <utility>

namespace doris {

GcpS3Client::GcpS3Client(const GcpCredentialConfig& credential, std::string ca_cert_path,
                         Aws::Client::ClientConfiguration aws_config,
                         Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy payload_signing_policy,
                         bool use_virtual_addressing)
        : Aws::S3::S3Client(std::make_shared<Aws::Auth::AnonymousAWSCredentialsProvider>(),
                            std::move(aws_config), payload_signing_policy, use_virtual_addressing),
          _token_provider(credential, ca_cert_path) {}

std::optional<Aws::S3::S3Error> GcpS3Client::authorize_request(
        Aws::AmazonWebServiceRequest& request) const {
    auto token = fetch_token();
    if (!token.has_value() || token->empty()) {
        // Return through the SDK outcome path before any object-storage HTTP request.
        // The token provider has already logged the underlying credential error.
        Aws::S3::S3Error error(Aws::S3::S3Errors::ACCESS_DENIED, "GcpAuthenticationError",
                               "Failed to obtain GCP access token", false);
        error.SetResponseCode(Aws::Http::HttpResponseCode::UNAUTHORIZED);
        return error;
    }
    request.SetAdditionalCustomHeaderValue("Authorization",
                                           Aws::String("Bearer ") + token->c_str());
    return std::nullopt;
}

Aws::S3::Model::AbortMultipartUploadOutcome GcpS3Client::AbortMultipartUpload(
        const Aws::S3::Model::AbortMultipartUploadRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::AbortMultipartUpload(authorized_request);
}

Aws::S3::Model::CompleteMultipartUploadOutcome GcpS3Client::CompleteMultipartUpload(
        const Aws::S3::Model::CompleteMultipartUploadRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::CompleteMultipartUpload(authorized_request);
}

Aws::S3::Model::CreateMultipartUploadOutcome GcpS3Client::CreateMultipartUpload(
        const Aws::S3::Model::CreateMultipartUploadRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::CreateMultipartUpload(authorized_request);
}

Aws::S3::Model::DeleteObjectOutcome GcpS3Client::DeleteObject(
        const Aws::S3::Model::DeleteObjectRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::DeleteObject(authorized_request);
}

Aws::S3::Model::DeleteObjectsOutcome GcpS3Client::DeleteObjects(
        const Aws::S3::Model::DeleteObjectsRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::DeleteObjects(authorized_request);
}

Aws::S3::Model::GetBucketLifecycleConfigurationOutcome GcpS3Client::GetBucketLifecycleConfiguration(
        const Aws::S3::Model::GetBucketLifecycleConfigurationRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::GetBucketLifecycleConfiguration(authorized_request);
}

Aws::S3::Model::GetBucketVersioningOutcome GcpS3Client::GetBucketVersioning(
        const Aws::S3::Model::GetBucketVersioningRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::GetBucketVersioning(authorized_request);
}

Aws::S3::Model::GetObjectOutcome GcpS3Client::GetObject(
        const Aws::S3::Model::GetObjectRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::GetObject(authorized_request);
}

Aws::S3::Model::HeadObjectOutcome GcpS3Client::HeadObject(
        const Aws::S3::Model::HeadObjectRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::HeadObject(authorized_request);
}

Aws::S3::Model::ListObjectsV2Outcome GcpS3Client::ListObjectsV2(
        const Aws::S3::Model::ListObjectsV2Request& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::ListObjectsV2(authorized_request);
}

Aws::S3::Model::PutObjectOutcome GcpS3Client::PutObject(
        const Aws::S3::Model::PutObjectRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::PutObject(authorized_request);
}

Aws::S3::Model::UploadPartOutcome GcpS3Client::UploadPart(
        const Aws::S3::Model::UploadPartRequest& request) const {
    auto authorized_request = request;
    if (auto error = authorize_request(authorized_request)) {
        return std::move(*error);
    }
    return Aws::S3::S3Client::UploadPart(authorized_request);
}

std::optional<std::string> GcpS3Client::fetch_token() const {
    return _token_provider.get_token();
}

} // namespace doris
