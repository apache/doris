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

#include <cstdint>
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
#include "cpp/obj-client/auth/azure_credential_options.h"
#include "cpp/obj-client/obj_storage_client.h"

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace bvar {
template <typename T>
class Adder;
}

namespace doris {

std::string hide_access_key(const std::string& ak);

class S3URI;
struct S3ClientConf {
    std::string endpoint;
    std::string region;
    std::string ak;
    std::string sk;
    std::string token;
    AzureCredentialOptions azure_credentials;
    // For azure we'd better support the bucket at the first time init azure blob container client
    std::string bucket;
    io::ObjStorageProvider provider = io::ObjStorageProvider::AWS;
    int max_connections = -1;
    int request_timeout_ms = -1;
    int connect_timeout_ms = -1;
    bool use_virtual_addressing = true;
    // For aws s3, no need to override endpoint
    bool need_override_endpoint = true;

    CredProviderType cred_provider_type = CredProviderType::Default;
    std::string role_arn;
    std::string external_id;
    // True when this client is bound to a Doris internal object storage bucket
    // (a storage vault in cloud mode). S3ClientFactory wraps such clients with the
    // shared rate limiter; external buckets (S3 load, TVF, external catalogs) are
    // returned bare in cloud mode.
    bool is_internal_bucket = false;

    // Compare provider-owned identity, never just a hash. Azure authentication
    // must not borrow AWS fields or depend on an unrelated region/role setting.
    bool operator==(const S3ClientConf&) const;
    uint64_t get_hash() const;
    std::string to_string() const;
};

struct S3ClientConfHash {
    size_t operator()(const S3ClientConf& conf) const {
        return static_cast<size_t>(conf.get_hash());
    }
};

struct S3Conf {
    std::string bucket;
    std::string prefix;
    S3ClientConf client_conf;

    bool sse_enabled = false;
    static S3Conf get_s3_conf(const cloud::ObjectStoreInfoPB&);
    static S3Conf get_s3_conf(const TS3StorageParam&);

    std::string to_string() const {
        return fmt::format("(bucket={}, prefix={}, client_conf={}, sse_enabled={})", bucket, prefix,
                           client_conf.to_string(), sse_enabled);
    }
};

class S3ClientFactory {
public:
    ~S3ClientFactory();

    static S3ClientFactory& instance();

    Result<std::shared_ptr<io::ObjStorageClient>> create(const S3ClientConf& s3_conf);

    static Status convert_properties_to_s3_conf(const std::map<std::string, std::string>& prop,
                                                const S3URI& s3_uri, S3Conf* s3_conf);

    static Status validate_azure_uri(const S3URI& uri, const S3ClientConf& conf);

    // Reused file systems must validate credentials before opening a new reader
    // or writer too. This does not expire or interrupt existing readers.
    static Status validate_credentials_for_access(const S3ClientConf& conf);

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

    AwsCredentialResult create_aws_credentials_provider(const S3ClientConf& s3_conf);

#ifdef BE_TEST
    void set_client_creator_for_test(
            std::function<std::shared_ptr<io::ObjStorageClient>(const S3ClientConf&)> creator);

    void clear_client_creator_for_test();
#endif

private:
#ifdef BE_TEST
    FRIEND_TEST(S3ClientFactoryTest, RefreshCaCertForCredentialsProvider);
    friend class AzureClientFactoryCacheTest;
#endif
    Result<std::shared_ptr<io::ObjStorageClient>> _create_s3_client(const S3ClientConf& s3_conf);
    Result<std::shared_ptr<io::ObjStorageClient>> _create_azure_client(const S3ClientConf& s3_conf);
    std::string _get_ca_cert_file_path();
    // Caller holds _lock. Eviction releases only the cache's shared ownership;
    // readers and writers keep their own client references.
    void _prune_azure_clients(int64_t now_ms);
    S3ClientFactory();

    Aws::SDKOptions _aws_options;
    std::mutex _lock;
    std::unordered_map<S3ClientConf, std::shared_ptr<io::ObjStorageClient>, S3ClientConfHash>
            _cache;
    struct AzureCachedClient {
        std::shared_ptr<io::ObjStorageClient> client;
        int64_t expiry_ms;
        uint64_t last_access;
    };
    // Azure's short-lived credentials rotate. Keep its bounded cache separate
    // from the existing non-Azure cache, whose eviction semantics are unchanged.
    std::unordered_map<S3ClientConf, AzureCachedClient, S3ClientConfHash> _azure_cache;
    size_t _azure_cache_capacity = 256;
    uint64_t _azure_cache_clock = 0;
    std::mutex _ca_cert_lock;
    std::string _ca_cert_file_path;
#ifdef BE_TEST
    std::function<std::shared_ptr<io::ObjStorageClient>(const S3ClientConf&)> _test_client_creator;
#endif
};

} // end namespace doris
