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
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Errors.h>
#include <bvar/bvar.h>
#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/cloud.pb.h>
#ifdef BE_TEST
#include <gtest/gtest_prod.h>
#endif

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "core/string_ref.h"
#include "cpp/aws_common.h"
#include "cpp/obj-client/auth/aws_credential_factory.h"
#include "cpp/obj-client/obj_storage_client.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace bvar {
template <typename T>
class Adder;
}

namespace doris::io {
struct S3AuthConfig;
} // namespace doris::io

namespace doris {

std::string hide_access_key(const std::string& ak);

// Owns the AWS SDK lifetime and the AWS helpers shared by object storage clients and other AWS
// users (e.g. Kinesis). Object storage clients themselves come from io::ObjClientFactory.
class S3ClientFactory {
public:
    ~S3ClientFactory();

    static S3ClientFactory& instance();

    static Aws::Client::ClientConfiguration& getClientConfiguration() {
        // The default constructor of ClientConfiguration will do some http call
        // such as Aws::Internal::GetEC2MetadataClient and other init operation,
        // which is unnecessary.
        // So here we use a static instance, and deep copy every time
        // to avoid unnecessary operations.
        static Aws::Client::ClientConfiguration instance;
        instance.requestTimeoutMs = config::aws_client_request_timeout_ms;
        return instance;
    }

    AwsCredentialResult create_aws_credentials_provider(const io::S3AuthConfig& auth);

    // The first valid path of config::ca_cert_file_paths, resolved lazily and cached; empty
    // when none is valid yet, in which case the next call resolves it again.
    std::string ca_cert_file_path() { return _get_ca_cert_file_path(); }

private:
#ifdef BE_TEST
    FRIEND_TEST(S3ClientFactoryTest, RefreshCaCertForCredentialsProvider);
#endif
    std::string _get_ca_cert_file_path();
    S3ClientFactory();

    Aws::SDKOptions _aws_options;
    std::mutex _ca_cert_lock;
    std::string _ca_cert_file_path;
};

} // end namespace doris
