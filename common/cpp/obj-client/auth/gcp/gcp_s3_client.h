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

#include <aws/core/AmazonWebServiceRequest.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>

#include <optional>
#include <string>

#include "cpp/obj-client/auth/gcp/gcp_auth.h"
#include "cpp/obj-client/auth/gcp/gcp_token_provider.h"

namespace doris {

// GCS accepts OAuth2 bearer tokens on its S3-compatible XML API. The client uses
// anonymous AWS credentials so AWSAuthV4Signer returns without adding SigV4, and
// each object-storage operation obtains its bearer token before entering the SDK.
// Keep these overrides in sync with the operations used by S3ObjStorageClient.
class GcpS3Client : public Aws::S3::S3Client {
public:
    GcpS3Client(const GcpCredentialConfig& credential, std::string ca_cert_path,
                Aws::Client::ClientConfiguration aws_config,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy payload_signing_policy,
                bool use_virtual_addressing);

    Aws::S3::Model::AbortMultipartUploadOutcome AbortMultipartUpload(
            const Aws::S3::Model::AbortMultipartUploadRequest& request) const override;

    Aws::S3::Model::CompleteMultipartUploadOutcome CompleteMultipartUpload(
            const Aws::S3::Model::CompleteMultipartUploadRequest& request) const override;

    Aws::S3::Model::CreateMultipartUploadOutcome CreateMultipartUpload(
            const Aws::S3::Model::CreateMultipartUploadRequest& request) const override;

    Aws::S3::Model::DeleteObjectOutcome DeleteObject(
            const Aws::S3::Model::DeleteObjectRequest& request) const override;

    Aws::S3::Model::DeleteObjectsOutcome DeleteObjects(
            const Aws::S3::Model::DeleteObjectsRequest& request) const override;

    Aws::S3::Model::GetBucketLifecycleConfigurationOutcome GetBucketLifecycleConfiguration(
            const Aws::S3::Model::GetBucketLifecycleConfigurationRequest& request) const override;

    Aws::S3::Model::GetBucketVersioningOutcome GetBucketVersioning(
            const Aws::S3::Model::GetBucketVersioningRequest& request) const override;

    Aws::S3::Model::GetObjectOutcome GetObject(
            const Aws::S3::Model::GetObjectRequest& request) const override;

    Aws::S3::Model::HeadObjectOutcome HeadObject(
            const Aws::S3::Model::HeadObjectRequest& request) const override;

    Aws::S3::Model::ListObjectsV2Outcome ListObjectsV2(
            const Aws::S3::Model::ListObjectsV2Request& request) const override;

    Aws::S3::Model::PutObjectOutcome PutObject(
            const Aws::S3::Model::PutObjectRequest& request) const override;

    Aws::S3::Model::UploadPartOutcome UploadPart(
            const Aws::S3::Model::UploadPartRequest& request) const override;

protected:
    std::optional<Aws::S3::S3Error> authorize_request(Aws::AmazonWebServiceRequest& request) const;

    // Test seam for asserting the outgoing header without relying on ADC or metadata.
    virtual std::optional<std::string> fetch_token() const;

private:
    GcpTokenProvider _token_provider;
};

} // namespace doris
